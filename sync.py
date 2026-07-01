"""
Ingestion: pulls Instantly data into the local schema.

Order: campaigns → per-campaign overview/daily/steps → leads → accounts/warmup.
Field names mirror the real Instantly schema. CRM-owned fields (stage,
disposition, notes, wake_date) are set on first insert and preserved on re-sync.
"""

import json
import logging
from datetime import datetime, timedelta, date

import re
import time

_EMAIL_PAT = re.compile(r'\S+@\S+\.\S+')

def _clean_name(s):
    """Strip email addresses that Instantly sometimes leaks into name fields."""
    return _EMAIL_PAT.sub('', s or '').strip()

from models import (
    db, Campaign, CampaignStep, DailyMetric, Prospect, Event,
    SendingAccount, User, Meta, EmailMessage, CHANNEL_EMAIL, WARM_THRESHOLD,
)
from instantly_client import InstantlyClient, INTEREST_TO_DISPOSITION

log = logging.getLogger(__name__)


def _dt(s):
    """Parse Instantly ISO timestamp → datetime (naive UTC)."""
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except (ValueError, AttributeError):
        return None


def _subject_map(campaign_raw):
    """Build {(step, variant): subject} from a campaign's sequences."""
    out = {}
    for seq in campaign_raw.get("sequences", []) or []:
        for i, step in enumerate(seq.get("steps", []) or []):
            for j, variant in enumerate(step.get("variants", []) or []):
                out[(str(i), str(j))] = (variant.get("subject") or "").strip()
    return out


def _warm_score(opens, clicks, replies, interest_code, last_activity):
    """Engagement score with recency decay. Stacks across signals."""
    base = opens * 1.0 + clicks * 2.0 + replies * 5.0
    if interest_code in (1, 2, 3, 4):        # interested / meeting / closed
        base += 10
    elif interest_code in (-1, -2, -3):       # not interested / wrong / lost
        base -= 5
    if last_activity:
        days = (datetime.utcnow() - last_activity).days
        recency = max(0.0, 1.0 - days / 45.0)
    else:
        recency = 0.5
    return round(max(0.0, base) * recency, 1)


def _derive_stage(reply_count, open_count, interest_code, contacted):
    """Initial pipeline stage from Instantly signals (insert-time only)."""
    if interest_code in (2, 3):
        return "Meeting"
    if interest_code == 4:
        return "Won"
    if interest_code == 1 or reply_count > 0:
        return "Replied"
    if interest_code in (-1, -3):
        return "Nurture"
    if open_count > 0:
        return "Engaged"
    if contacted:
        return "Contacted"
    return "New"


def _strip_html(html: str) -> str:
    """Strip HTML, preserving paragraph structure as real newlines."""
    h = html or ""
    # Block elements → newline before stripping tags
    h = re.sub(r"<br\s*/?>", "\n", h, flags=re.I)
    h = re.sub(r"</(?:p|div|tr|li|h[1-6])>", "\n", h, flags=re.I)
    # Remove all remaining tags
    h = re.sub(r"<[^>]+>", "", h)
    # Decode common HTML entities
    h = h.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<") \
         .replace("&gt;", ">").replace("&#8217;", "'").replace("&#8216;", "'") \
         .replace("&#8220;", '"').replace("&#8221;", '"') \
         .replace("&#8211;", "-").replace("&#8212;", "-")
    h = re.sub(r"&[a-z#0-9]+;", "", h)
    # Trim trailing spaces per line, collapse 3+ blank lines to 2
    lines = [l.rstrip() for l in h.splitlines()]
    h = "\n".join(lines)
    h = re.sub(r"\n{3,}", "\n\n", h)
    return h.strip()


def _extract_new_content(html: str) -> str:
    """Return only the NEW portion of an email body, stripping the quoted thread.

    Email clients include all prior messages in the HTML. We want only what
    the sender wrote fresh — everything before the first quote block.

    Handles:
      - Gmail/Outlook 'On [date] ... wrote:' quote headers
      - blockquote tags (nearly universal)
      - '> ' plain-text quoting
      - '----- Original Message -----' / '-------- Forwarded' dividers
    """
    if not html:
        return ""

    # 1. Remove blockquote sections (quoted content in HTML email)
    cleaned = re.sub(r"<blockquote[^>]*>.*?</blockquote>", "", html, flags=re.S | re.I)

    # 2. Remove common reply-header dividers that precede the blockquote
    #    e.g. <div class="gmail_quote">...</div>
    cleaned = re.sub(r'<div[^>]+class="[^"]*(?:gmail_quote|OutlookMessageHeader)[^"]*"[^>]*>.*?</div>', "", cleaned, flags=re.S | re.I)

    # 3. Strip remaining HTML tags and normalise whitespace
    text = _strip_html(cleaned)

    # 4. Plain-text quote removal: stop at "On [date], ... wrote:" pattern
    #    This catches cases where the HTML was already stripped before we got it.
    cutoff = re.search(
        r"\bOn\s+\w+ \d+[\s,].*?wrote\s*:", text, flags=re.S
    )
    if cutoff:
        text = text[:cutoff.start()].strip()

    # 5. Stop at common divider strings
    for divider in ("-----Original Message-----", "________", "-------- Forwarded", "From:", "Sent:"):
        idx = text.find(divider)
        if idx > 30:  # avoid trimming too early if it starts with "From:"
            text = text[:idx].strip()
            break

    return text.strip()


def sync_emails(client: InstantlyClient) -> dict:
    """Incremental email sync: fetch newest-first, stop when we hit a known ID.

    Syncs both outbound (ue_type=1) and inbound (ue_type=2) emails so the
    thread view can show the full conversation.
    """
    summary = {"new": 0, "pages": 0, "errors": []}
    starting_after = None
    MAX_PAGES = 30  # safety cap on first-ever sync (~3000 emails)

    while summary["pages"] < MAX_PAGES:
        try:
            items, nsa = client.list_emails_page(starting_after=starting_after)
        except Exception as e:
            summary["errors"].append(str(e))
            break

        if not items:
            break

        summary["pages"] += 1
        hit_known = False

        for item in items:
            mid = item.get("id")
            if not mid:
                continue

            # Stop when we reach an already-synced email
            if db.session.get(EmailMessage, mid) is not None:
                hit_known = True
                continue

            body = item.get("body") or {}
            html = body.get("html") or ""
            em = EmailMessage(
                id=mid,
                thread_id=item.get("thread_id"),
                lead_id=item.get("lead_id"),
                prospect_email=None,    # resolved below via lead
                campaign_id=item.get("campaign_id"),
                eaccount=item.get("eaccount"),
                from_address=item.get("from_address_email"),
                to_address=(item.get("to_address_email_list") or ""),
                subject=(item.get("subject") or "").strip(),
                body_html=html,
                body_text=_extract_new_content(html)[:2000],
                ue_type=item.get("ue_type", 1),
                is_unread=bool(item.get("is_unread")),
                is_focused=bool(item.get("is_focused")),
                step=str(item.get("step")) if item.get("step") is not None else None,
                timestamp_email=_dt(item.get("timestamp_email")),
                timestamp_created=_dt(item.get("timestamp_created")),
            )
            # Resolve prospect_email
            if em.lead_id:
                p = db.session.get(Prospect, em.lead_id)
                if p:
                    em.prospect_email = p.email
            # For inbound emails (prospect replies), from_address IS the prospect
            if not em.prospect_email:
                if em.ue_type == 2:
                    em.prospect_email = em.from_address
                else:
                    em.prospect_email = (em.to_address or "").split(",")[0].strip() or None

            db.session.add(em)
            summary["new"] += 1

        # Commit each page to reduce memory pressure
        db.session.commit()

        # Stop once every item on this page was already in the DB
        if hit_known and summary["new"] == 0:
            break
        if hit_known:
            break  # saw an old record mid-page; newer ones are new, but we're done

        if not nsa:
            break

        starting_after = nsa
        time.sleep(0.8)   # avoid rate-limit on large initial syncs

    return summary


def run_sync(client: InstantlyClient, since_days: int = 60, account_id: int = None) -> dict:
    """Full sync. Returns a summary dict."""
    summary = {"campaigns": 0, "leads": 0, "events": 0, "accounts": 0, "emails": 0, "errors": []}

    today = date.today()
    start = today - timedelta(days=since_days)

    # ── 0. Workspace identity ─────────────────────────────────────────────────
    ws = client.workspace_current()
    if ws.get("name"):
        Meta.set("workspace_name", ws["name"])
    Meta.set("last_sync", datetime.utcnow().isoformat())

    # ── 1. Campaigns ──────────────────────────────────────────────────────────
    try:
        campaigns = client.list_campaigns()
    except Exception as e:
        summary["errors"].append(f"campaigns: {e}")
        return summary

    for craw in campaigns:
        cid = craw.get("id")
        if not cid:
            continue
        c = db.session.get(Campaign, cid) or Campaign(id=cid)
        if account_id:
            c.account_id = account_id
        c.name = craw.get("name") or "Unnamed"
        c.channel = CHANNEL_EMAIL
        c.status_code = craw.get("status", 0)
        c.status_label = client.campaign_status_label(craw.get("status"))
        scheds = (craw.get("campaign_schedule") or {}).get("schedules") or []
        c.timezone = (scheds[0].get("timezone") if scheds else None) or "UTC"
        c.sending_accounts = json.dumps(craw.get("email_list") or [])
        c.open_tracking = bool(craw.get("open_tracking", True))
        c.link_tracking = bool(craw.get("link_tracking", False))

        # Owner
        owner = craw.get("owned_by")
        if owner and not db.session.get(User, owner):
            db.session.add(User(id=owner, name="(Instantly user)"))
        db.session.add(c)
        db.session.flush()

        # ── Overview analytics ──
        pi = client.parse_int
        try:
            ov = client.campaign_overview(cid)
        except Exception as e:
            ov = {}
            summary["errors"].append(f"overview {c.name}: {e}")
        if ov:
            c.emails_sent_count = pi(ov.get("emails_sent_count"))
            c.contacted_count = pi(ov.get("contacted_count"))
            c.new_leads_contacted_count = pi(ov.get("new_leads_contacted_count"))
            c.open_count = pi(ov.get("open_count"))
            c.open_count_unique = pi(ov.get("open_count_unique"))
            c.link_click_count = pi(ov.get("link_click_count"))
            c.link_click_count_unique = pi(ov.get("link_click_count_unique"))
            c.reply_count = pi(ov.get("reply_count"))
            c.reply_count_unique = pi(ov.get("reply_count_unique"))
            c.bounced_count = pi(ov.get("bounced_count"))
            c.unsubscribed_count = pi(ov.get("unsubscribed_count"))
            c.total_opportunities = pi(ov.get("total_opportunities"))
            c.total_opportunity_value = float(ov.get("total_opportunity_value") or 0)
            c.total_interested = pi(ov.get("total_interested"))
            c.total_meeting_booked = pi(ov.get("total_meeting_booked"))
            c.total_meeting_completed = pi(ov.get("total_meeting_completed"))
            c.total_closed = pi(ov.get("total_closed"))
        c.synced_at = datetime.utcnow()

        # ── Steps (with subjects) ──
        CampaignStep.query.filter_by(campaign_id=cid).delete()
        submap = _subject_map(craw)
        try:
            steps = client.campaign_steps(cid)
        except Exception as e:
            steps = []
            summary["errors"].append(f"steps {c.name}: {e}")
        for s in steps:
            step_k, var_k = str(s.get("step")), str(s.get("variant") or "0")
            subj = submap.get((step_k, var_k), "")
            db.session.add(CampaignStep(
                campaign_id=cid, step=step_k, variant=var_k,
                subject=subj or "(follow-up: same thread)",
                sent=pi(s.get("sent")), opened=pi(s.get("opened")),
                unique_opened=pi(s.get("unique_opened")),
                replies=pi(s.get("replies")), unique_replies=pi(s.get("unique_replies")),
                replies_automatic=pi(s.get("replies_automatic")),
                clicks=pi(s.get("clicks")), unique_clicks=pi(s.get("unique_clicks")),
            ))

        # ── Daily metrics ──
        DailyMetric.query.filter_by(campaign_id=cid).delete()
        try:
            daily = client.campaign_daily(cid, str(start), str(today))
        except Exception as e:
            daily = []
            summary["errors"].append(f"daily {c.name}: {e}")
        for d in daily:
            if not d.get("date"):
                continue
            db.session.add(DailyMetric(
                campaign_id=cid, date=d["date"],
                sent=pi(d.get("sent")), contacted=pi(d.get("contacted")),
                new_leads_contacted=pi(d.get("new_leads_contacted")),
                opened=pi(d.get("opened")), unique_opened=pi(d.get("unique_opened")),
                replies=pi(d.get("replies")), unique_replies=pi(d.get("unique_replies")),
                clicks=pi(d.get("clicks")), unique_clicks=pi(d.get("unique_clicks")),
                opportunities=pi(d.get("opportunities")),
            ))

        summary["campaigns"] += 1

    # ── 2. Leads → prospects + events ─────────────────────────────────────────
    try:
        leads = client.list_leads()
    except Exception as e:
        summary["errors"].append(f"leads: {e}")
        leads = []

    lead_total = {}
    for lraw in leads:
        email = (lraw.get("email") or "").strip().lower()
        if not email:
            continue
        lid = lraw.get("id") or f"{lraw.get('campaign')}::{email}"
        pi = client.parse_int

        p = db.session.get(Prospect, lid)
        is_new = p is None
        if is_new:
            p = Prospect(id=lid)
        if account_id:
            p.account_id = account_id

        p.email = email
        p.first_name = _clean_name(lraw.get("first_name"))
        p.last_name = _clean_name(lraw.get("last_name"))
        p.company_name = lraw.get("company_name") or ""
        p.company_domain = lraw.get("company_domain") or ""
        p.campaign_id = lraw.get("campaign")
        lead_total[p.campaign_id] = lead_total.get(p.campaign_id, 0) + 1

        # Custom payload: phone, linkedIn, jobTitle
        payload = lraw.get("payload") or {}
        p.phone = payload.get("phone") or lraw.get("phone") or ""
        p.linkedin_url = payload.get("linkedIn") or payload.get("linkedin") or ""
        p.job_title = payload.get("jobTitle") or ""
        p.owner_id = lraw.get("assigned_to") or None

        # Engagement
        p.email_open_count = pi(lraw.get("email_open_count"))
        p.email_click_count = pi(lraw.get("email_click_count"))
        p.email_reply_count = pi(lraw.get("email_reply_count"))
        p.instantly_status = lraw.get("status", 0)
        p.instantly_status_label = client.lead_status_label(lraw.get("status"))

        interest_code = lraw.get("lt_interest_status")
        p.lt_interest_status = interest_code
        p.interest_label = client.interest_label(interest_code)

        p.timestamp_last_open = _dt(lraw.get("timestamp_last_open"))
        p.timestamp_last_reply = _dt(lraw.get("timestamp_last_reply"))
        p.timestamp_last_contact = _dt(lraw.get("timestamp_last_contact"))
        last_activity = p.timestamp_last_reply or p.timestamp_last_open or p.timestamp_last_contact
        p.last_activity_at = last_activity
        p.warm_score = _warm_score(
            p.email_open_count, p.email_click_count, p.email_reply_count,
            interest_code, last_activity)

        # CRM fields: set on insert, preserve user edits on re-sync
        if is_new:
            p.stage = _derive_stage(p.email_reply_count, p.email_open_count,
                                    interest_code, bool(p.timestamp_last_contact))
            # Auto-disposition + wake from Instantly's AI interest
            label = client.interest_label(interest_code)
            if label and label in INTEREST_TO_DISPOSITION:
                disp, wake_days = INTEREST_TO_DISPOSITION[label]
                p.disposition = disp
                if wake_days:
                    p.wake_date = today + timedelta(days=wake_days)
        else:
            # Re-sync: advance stage when signals clearly warrant it.
            # Never downgrade. Never touch terminal stages (Lost, Nurture, Won).
            _STAGE_RANK = {"New": 0, "Contacted": 1, "Engaged": 2,
                           "Replied": 3, "Meeting": 4, "Won": 5}
            cur = _STAGE_RANK.get(p.stage, -1)
            if interest_code in (2, 3):              # meeting booked / completed
                tgt, trk = "Meeting", 4
            elif interest_code == 4:                  # closed / won
                tgt, trk = "Won", 5
            elif p.email_reply_count > 0 or interest_code == 1:  # replied or interested
                tgt, trk = "Replied", 3
            elif p.email_open_count > 0 and cur == 0:  # opened, still New
                tgt, trk = "Engaged", 2
            else:
                tgt, trk = None, -1
            if tgt and trk > cur and p.stage not in ("Lost", "Nurture"):
                p.stage = tgt
                p.stage_changed_at = datetime.utcnow()

        db.session.add(p)

        # ── Events from prospect timestamps (real timestamps only) ──
        def add_event(etype, when, meta=None):
            if not when:
                return 0
            exists = Event.query.filter_by(
                prospect_email=email, type=etype, occurred_at=when,
                campaign_id=p.campaign_id).first()
            if exists:
                return 0
            db.session.add(Event(
                prospect_id=lid, prospect_email=email, campaign_id=p.campaign_id,
                account_id=account_id,
                channel=CHANNEL_EMAIL, type=etype, occurred_at=when,
                source="sync", meta=json.dumps(meta or {})))
            return 1

        summary["events"] += add_event("contacted", p.timestamp_last_contact)
        summary["events"] += add_event("email_opened", p.timestamp_last_open,
                                       {"open_count": p.email_open_count})
        if interest_code is not None:
            summary["events"] += add_event(
                "interest_" + (p.interest_label or "set").lower().replace(" ", "_"),
                _dt(lraw.get("timestamp_last_interest_change")))

        summary["leads"] += 1

    # Lead counts per campaign
    for cid, n in lead_total.items():
        c = db.session.get(Campaign, cid)
        if c:
            c.total_leads = n

    # ── 3. Sending accounts + warmup ──────────────────────────────────────────
    try:
        accounts = client.list_accounts()
        emails = [a.get("email") for a in accounts if a.get("email")]
        wa = client.warmup_analytics(emails)
        agg = wa.get("aggregate_data", {}) if wa else {}
    except Exception as e:
        accounts, agg = [], {}
        summary["errors"].append(f"accounts: {e}")

    for araw in accounts:
        email = araw.get("email")
        if not email:
            continue
        a = db.session.get(SendingAccount, email) or SendingAccount(email=email)
        if account_id:
            a.account_id = account_id
        a.status_code = araw.get("status", 0)
        a.warmup_status = araw.get("warmup_status", 0)
        a.warmup_score = client.parse_int(araw.get("stat_warmup_score"))
        a.provider_code = araw.get("provider_code", 0)
        a.setup_pending = bool(araw.get("setup_pending", False))
        if a.status_code < 0:
            sm = araw.get("status_message")
            a.error_message = ((sm.get("e_message") or sm.get("response") or "Connection error")[:500]
                               if isinstance(sm, dict) and sm else "Connection error")
        else:
            a.error_message = ""

        wd = agg.get(email, {})
        a.health_score = client.parse_int(wd.get("health_score"))
        a.wa_sent = client.parse_int(wd.get("sent"))
        a.wa_landed_inbox = client.parse_int(wd.get("landed_inbox"))
        a.wa_landed_spam = client.parse_int(wd.get("landed_spam"))
        a.wa_received = client.parse_int(wd.get("received"))
        a.synced_at = datetime.utcnow()
        db.session.add(a)
        summary["accounts"] += 1

    # ── 4. Emails (inbox + thread history) ────────────────────────────────────────
    try:
        es = sync_emails(client)
        summary["emails"] = es["new"]
        if es["errors"]:
            summary["errors"].extend(es["errors"])
    except Exception as e:
        summary["errors"].append(f"emails: {e}")
        summary["emails"] = 0

    db.session.commit()
    return summary
