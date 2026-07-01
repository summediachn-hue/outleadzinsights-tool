"""
Analytics: all computations over the synced local data.
Timing is timezone-aware: open/reply timestamps (stored UTC) are converted to
each campaign's own timezone, so "best day/time" reads in recipient-local time.
"""

import json
from collections import defaultdict, Counter
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import func
from models import (
    db, Campaign, CampaignStep, DailyMetric, Prospect, SendingAccount,
    HeyReachCampaign, HeyReachLead, Event,
    WARM_THRESHOLD, ROTTING_DAYS,
)

BENCH_OPEN = 30.0
BENCH_REPLY = 4.0
BOUNCE_DANGER = 5.0

DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
TIME_BUCKETS = [
    ("Early", 5, 9), ("Morning", 9, 12), ("Midday", 12, 15),
    ("Afternoon", 15, 18), ("Evening", 18, 22), ("Night", 22, 5),
]

_UTC = ZoneInfo("UTC")
_tz_cache: Dict[str, ZoneInfo] = {}


def _zone(name):
    z = _tz_cache.get(name)
    if z is None:
        try:
            z = ZoneInfo(name or "UTC")
        except Exception:
            z = _UTC
        _tz_cache[name] = z
    return z


def _to_local(dt, tzname):
    """Naive-UTC datetime → aware local datetime in tzname."""
    if not dt:
        return None
    return dt.replace(tzinfo=_UTC).astimezone(_zone(tzname))


def _bucket_for_hour(h: int) -> str:
    for name, start, end in TIME_BUCKETS:
        if start < end:
            if start <= h < end:
                return name
        else:
            if h >= start or h < end:
                return name
    return "Night"


def _pct(num, denom):
    return round(num / denom * 100, 1) if denom else 0.0


def _acct_campaign_ids(account_id):
    if not account_id:
        return None
    return {c.id for c in Campaign.query.filter_by(account_id=account_id).all()}


def _tz_map(account_id=None):
    q = Campaign.query
    if account_id:
        q = q.filter_by(account_id=account_id)
    return {c.id: (c.timezone or "UTC") for c in q.all()}


def _campaign_totals(account_id=None):
    cols = [
        func.sum(Campaign.emails_sent_count), func.sum(Campaign.contacted_count),
        func.sum(Campaign.open_count), func.sum(Campaign.open_count_unique),
        func.sum(Campaign.reply_count_unique), func.sum(Campaign.bounced_count),
        func.sum(Campaign.unsubscribed_count), func.sum(Campaign.total_opportunities),
        func.sum(Campaign.total_opportunity_value), func.sum(Campaign.total_interested),
        func.sum(Campaign.total_meeting_booked), func.sum(Campaign.total_closed),
        func.sum(Campaign.link_click_count_unique),
    ]
    q = db.session.query(*cols)
    if account_id:
        q = q.filter(Campaign.account_id == account_id)
    r = q.first() or []
    keys = ["sent", "contacted", "opens", "opens_unique", "replies_unique",
            "bounced", "unsubscribed", "opportunities", "opp_value", "interested",
            "meetings", "closed", "clicks_unique"]
    return {k: (v or 0) for k, v in zip(keys, r)}


# ── Timeframe-aware engagement ─────────────────────────────────────────────────

def _window_totals(start: Optional[date], end: Optional[date], account_id=None):
    if start or end:
        q = db.session.query(
            func.sum(DailyMetric.sent), func.sum(DailyMetric.contacted),
            func.sum(DailyMetric.unique_opened), func.sum(DailyMetric.unique_replies),
            func.sum(DailyMetric.opportunities))
        if start:
            q = q.filter(DailyMetric.date >= start.isoformat())
        if end:
            q = q.filter(DailyMetric.date <= end.isoformat())
        if account_id:
            cids = _acct_campaign_ids(account_id)
            if cids:
                q = q.filter(DailyMetric.campaign_id.in_(cids))
            else:
                return {"sent": 0, "contacted": 0, "opens": 0, "replies": 0, "opportunities": 0}
        r = q.first()
        sent, contacted, opens, replies, opps = [int(x or 0) for x in (r or (0,) * 5)]
    else:
        t = _campaign_totals(account_id)
        sent, contacted = int(t["sent"]), int(t["contacted"])
        opens, replies, opps = int(t["opens_unique"]), int(t["replies_unique"]), int(t["opportunities"])
    return {"sent": sent, "contacted": contacted, "opens": opens,
            "replies": replies, "opportunities": opps}


def dashboard_metrics(start=None, end=None, account_id=None) -> Dict:
    w = _window_totals(start, end, account_id)
    return {**w, "open_rate": _pct(w["opens"], w["contacted"]),
            "reply_rate": _pct(w["replies"], w["contacted"])}


def lifetime_stats(account_id=None) -> Dict:
    t = _campaign_totals(account_id)
    cq = Campaign.query.filter_by(account_id=account_id) if account_id else Campaign.query
    pq = Prospect.query.filter_by(account_id=account_id) if account_id else Prospect.query
    return {
        "total_campaigns": cq.count(),
        "active_campaigns": cq.filter_by(status_code=1).count(),
        "total_leads": pq.count(),
        "warm_leads": pq.filter(Prospect.email_open_count >= WARM_THRESHOLD).count(),
        "interested": int(t["interested"]),
        "pipeline_value": int(t["opp_value"]),
        "repeat_open_ratio": round(t["opens"] / t["opens_unique"], 2) if t["opens_unique"] else 0,
    }


def funnel(start=None, end=None, account_id=None) -> List[Dict]:
    w = _window_totals(start, end, account_id)
    if start or end:
        stages = [("Emails Sent", w["sent"], "#5b54f0"), ("Contacted", w["contacted"], "#7c75f3"),
                  ("Opened", w["opens"], "#7c3aed"), ("Replied", w["replies"], "#0d9488"),
                  ("Opportunities", w["opportunities"], "#0a8f5b")]
    else:
        t = _campaign_totals(account_id)
        stages = [("Emails Sent", int(t["sent"]), "#5b54f0"), ("Contacted", int(t["contacted"]), "#7c75f3"),
                  ("Opened", int(t["opens_unique"]), "#7c3aed"), ("Replied", int(t["replies_unique"]), "#0d9488"),
                  ("Interested", int(t["interested"]), "#e08a1e"), ("Meetings", int(t["meetings"]), "#ea580c"),
                  ("Closed Won", int(t["closed"]), "#0a8f5b")]
    base = stages[1][1] or 1
    return [{"label": l, "value": v, "color": c, "pct_of_contacted": _pct(v, base)} for l, v, c in stages]


# ── Timezone-aware timing (consolidated) ───────────────────────────────────────

def engagement_timing(campaign_id: Optional[str] = None,
                      start: Optional[date] = None, end: Optional[date] = None,
                      account_id=None) -> Dict:
    """
    When opens & replies happen, in LOCAL time. Per-campaign uses that campaign's
    timezone; overall converts each event to its own campaign's timezone.
    """
    tzmap = _tz_map(account_id)
    names = [b[0] for b in TIME_BUCKETS]
    day_opens = {d: 0 for d in DAYS}
    day_replies = {d: 0 for d in DAYS}
    time_opens = {b: 0 for b in names}
    time_replies = {b: 0 for b in names}
    grid = {d: {b: 0 for b in names} for d in DAYS}
    grid_replies = {d: {b: 0 for b in names} for d in DAYS}

    def in_range(dt):
        if start and dt.date() < start:
            return False
        if end and dt.date() > end:
            return False
        return True

    qo = Prospect.query.filter(Prospect.timestamp_last_open.isnot(None))
    qr = Prospect.query.filter(Prospect.timestamp_last_reply.isnot(None))
    if campaign_id:
        qo = qo.filter_by(campaign_id=campaign_id)
        qr = qr.filter_by(campaign_id=campaign_id)
    elif account_id:
        qo = qo.filter_by(account_id=account_id)
        qr = qr.filter_by(account_id=account_id)

    for p in qo.all():
        if not in_range(p.timestamp_last_open):
            continue
        loc = _to_local(p.timestamp_last_open, tzmap.get(p.campaign_id, "UTC"))
        d, b = DAYS[loc.weekday()], _bucket_for_hour(loc.hour)
        day_opens[d] += 1
        time_opens[b] += 1
        grid[d][b] += 1
    for p in qr.all():
        if not in_range(p.timestamp_last_reply):
            continue
        loc = _to_local(p.timestamp_last_reply, tzmap.get(p.campaign_id, "UTC"))
        d, b = DAYS[loc.weekday()], _bucket_for_hour(loc.hour)
        day_replies[d] += 1
        time_replies[b] += 1
        grid_replies[d][b] += 1

    def best(d):
        return max(d, key=d.get) if any(d.values()) else None

    if campaign_id:
        tz_label = "times in " + tzmap.get(campaign_id, "UTC")
    else:
        tz_label = "each campaign's local time"

    return {
        "days": DAYS, "buckets": names,
        "bucket_ranges": {b[0]: f"{b[1]:02d}:00–{b[2]:02d}:00" for b in TIME_BUCKETS},
        "day_opens": day_opens, "day_replies": day_replies,
        "time_opens": time_opens, "time_replies": time_replies,
        "grid": grid, "grid_replies": grid_replies,
        "max_cell": max((grid[d][b] for d in DAYS for b in names), default=0) or 1,
        "max_cell_replies": max((grid_replies[d][b] for d in DAYS for b in names), default=0) or 1,
        "best_open_day": best(day_opens), "best_reply_day": best(day_replies),
        "best_open_time": best(time_opens), "best_reply_time": best(time_replies),
        "max_day_open": max(day_opens.values()) or 1, "max_day_reply": max(day_replies.values()) or 1,
        "max_time_open": max(time_opens.values()) or 1, "max_time_reply": max(time_replies.values()) or 1,
        "total_opens": sum(day_opens.values()), "total_replies": sum(day_replies.values()),
        "tz_label": tz_label,
    }


# ── Leads ──────────────────────────────────────────────────────────────────────

def get_warm_leads(limit: int = 20, campaign_id: Optional[str] = None, account_id=None) -> List[Prospect]:
    q = Prospect.query.filter(Prospect.email_open_count >= WARM_THRESHOLD)
    if campaign_id:
        q = q.filter_by(campaign_id=campaign_id)
    elif account_id:
        q = q.filter_by(account_id=account_id)
    return q.order_by(Prospect.warm_score.desc()).limit(limit).all()


def hidden_intent_leads(limit: int = 20, account_id=None) -> List[Prospect]:
    q = (Prospect.query.filter(Prospect.email_open_count >= WARM_THRESHOLD)
         .filter(Prospect.email_reply_count == 0))
    if account_id:
        q = q.filter_by(account_id=account_id)
    return q.order_by(Prospect.email_open_count.desc()).limit(limit).all()


# ── Campaign diagnosis ─────────────────────────────────────────────────────────

def diagnose_campaign(c: Campaign) -> Dict:
    if c.emails_sent_count == 0:
        return {"health": "unknown", "score": 0, "issues": ["No emails sent yet."], "recommendations": []}
    issues, recs, health = [], [], "good"
    o, r, b, u = c.open_rate, c.reply_rate, c.bounce_rate, c.unsub_rate
    if b > BOUNCE_DANGER:
        health = "critical"
        issues.append(f"High bounce rate ({b:.1f}%): sender reputation at risk.")
        recs += ["Validate your lead list before the next send.", "Check SPF/DKIM/DMARC for sending domains."]
    if o < 15:
        health = "critical"
        issues.append(f"Very low open rate ({o:.1f}%): likely landing in spam.")
        recs += ["Check deliverability (warmup health, spam placement).", "Reduce volume to recover reputation."]
    elif o < BENCH_OPEN:
        health = "warning" if health == "good" else health
        issues.append(f"Below-benchmark open rate ({o:.1f}% vs {BENCH_OPEN:.0f}%): subject lines.")
        recs += ["Test shorter, curiosity-driven subjects.", "Personalise the subject with {{firstName}} / company."]
    if o >= 15 and r < BENCH_REPLY:
        health = "warning" if health == "good" else health
        if o >= BENCH_OPEN:
            issues.append(f"People open but don't reply ({r:.1f}%): the body isn't converting.")
            recs += ["Lead with value, not your pitch.", "One low-friction CTA. Cut to <80 words."]
        else:
            issues.append(f"Low reply rate ({r:.1f}%): copy + deliverability both need work.")
    if u > 2:
        health = "warning" if health == "good" else health
        issues.append(f"High unsubscribe rate ({u:.1f}%): targeting may be off.")
        recs.append("Revisit your ICP: are you reaching the right personas?")
    if not issues:
        issues.append(f"Performing well (open {o:.1f}%, reply {r:.1f}%).")
        recs.append("Scale volume or clone to similar audiences.")
    score = min(100, int((min(o, 60) / 60) * 50 + (min(r, 10) / 10) * 50))
    return {"health": health, "score": score, "issues": issues, "recommendations": recs,
            "open_rate": o, "reply_rate": r, "bounce_rate": b, "unsub_rate": u}


# ── Campaign recommendation engine (data-backed, per-campaign) ─────────────────

_SEV_ORDER = {"critical": 0, "important": 1, "opportunity": 2, "good": 3}
_SEV_COLOR = {"critical": "red", "important": "orange", "opportunity": "accent", "good": "green"}


def _stepnum(s):
    try:
        return int(s)
    except (TypeError, ValueError):
        return 999


def _campaign_mailboxes(c: Campaign):
    try:
        emails = json.loads(c.sending_accounts or "[]")
    except (ValueError, TypeError):
        emails = []
    accts = [db.session.get(SendingAccount, e) for e in emails]
    accts = [a for a in accts if a]
    if not accts:
        return None
    return {
        "count": len(accts),
        "issues": [a for a in accts if a.is_issue],
    }


def _bottleneck(c: Campaign, mh) -> Dict:
    """Find the single weakest stage: that's the #1 lever."""
    o, r, b = c.open_rate, c.reply_rate, c.bounce_rate
    mailbox_bad = bool(mh and mh["issues"])
    if b > 5 or (o < 15 and (mailbox_bad or not mh)):
        return {"stage": "Deliverability",
                "verdict": "Your #1 problem is deliverability: emails aren't reliably reaching inboxes. Fix this before touching copy or subjects."}
    if o < BENCH_OPEN:
        return {"stage": "Open rate",
                "verdict": f"Your bottleneck is opens ({o:.1f}%). Deliverability looks fine, so the subject lines are the lever to pull."}
    if r < BENCH_REPLY:
        return {"stage": "Reply rate",
                "verdict": f"Opens are healthy ({o:.1f}%) but replies lag ({r:.1f}%). The bottleneck is the email copy and CTA: people see it, they're just not compelled to respond."}
    if c.total_interested == 0:
        return {"stage": "Conversion",
                "verdict": "People reply but none have converted to interested yet: the offer or targeting may not fit this audience."}
    return {"stage": "Scaling",
            "verdict": "This campaign performs well across the board. The lever now is volume: scale it."}


def campaign_recommendations(c: Campaign) -> Dict:
    """
    Synthesize every signal (deliverability, subjects, copy, sequence, A/B,
    timing, targeting, warm leads) into prioritized, data-backed actions.
    """
    sent = c.emails_sent_count
    if sent < 30:
        return {"summary": {"verdict": "Not enough data yet: send more before optimizing.",
                            "bottleneck": {"stage": "Data", "verdict": ""}, "health": "unknown"},
                "recommendations": []}

    o, r, b, u, rep = c.open_rate, c.reply_rate, c.bounce_rate, c.unsub_rate, c.repeat_open_ratio
    steps = (CampaignStep.query.filter_by(campaign_id=c.id)
             .filter(CampaignStep.sent > 0).all())
    steps.sort(key=lambda s: _stepnum(s.step))
    mh = _campaign_mailboxes(c)
    timing = engagement_timing(c.id)
    hidden = (Prospect.query.filter_by(campaign_id=c.id)
              .filter(Prospect.email_open_count >= WARM_THRESHOLD, Prospect.email_reply_count == 0).count())

    recs = []

    def add(cat, sev, title, detail, action, evidence):
        recs.append({"category": cat, "severity": sev, "color": _SEV_COLOR[sev],
                     "title": title, "detail": detail, "action": action, "evidence": evidence})

    # 1: Bounce / list hygiene
    if b > 5:
        add("Deliverability", "critical", f"High bounce rate at {b:.1f}%",
            f"{c.bounced_count} of {sent} emails bounced. Above 5% signals a dirty list and erodes sender reputation, which then suppresses opens across the board.",
            "Run the uncontacted leads through a verifier (MillionVerifier / ZeroBounce) and remove invalid + catch-all addresses before the next send.",
            f"{c.bounced_count} bounced / {sent} sent = {b:.1f}%")
    elif b > 2:
        add("Deliverability", "important", f"Bounce rate creeping up ({b:.1f}%)",
            f"{c.bounced_count} bounced: not critical yet, but worth cleaning before it crosses 5%.",
            "Verify the segment of the list that hasn't been contacted yet.",
            f"{b:.1f}% bounce")

    # 2: Mailbox issues (only REAL problems — errors or spam placement)
    if mh and mh["issues"]:
        errs = [a for a in mh["issues"] if a.health_state == "error"]
        spam = [a for a in mh["issues"] if a.health_state == "spam"]
        if errs:
            names = ", ".join(a.email for a in errs[:3])
            add("Deliverability", "critical",
                f"{len(errs)} sending mailbox(es) disconnected or erroring",
                f"{names} {'has' if len(errs) == 1 else 'have'} an account error in Instantly, so email from {'it' if len(errs) == 1 else 'them'} won't deliver.",
                "Reconnect / fix the mailbox in Instantly, or remove it from this campaign's sending pool.",
                f"{len(errs)} mailbox error(s)")
        if spam:
            names = ", ".join(a.email for a in spam[:3])
            add("Deliverability", "important",
                f"{len(spam)} mailbox(es) landing in spam",
                f"{names} {'is' if len(spam) == 1 else 'are'} placing warmup emails in spam, which suppresses opens on this campaign.",
                "Pause those mailboxes and let warmup recover before sending more.",
                f"{len(spam)} spam-risk mailbox(es)")

    # 3: Cross-signal: low opens but NO delivery issues → it's the subject
    if o < BENCH_OPEN and mh and not mh["issues"] and b < 3:
        add("Subject lines", "important" if o >= 15 else "critical",
            f"Open rate {o:.1f}% is low, but delivery looks clean",
            f"No mailbox errors and bounce is low ({b:.1f}%), so emails are reaching inboxes: people just aren't opening. That's a subject-line problem, not spam.",
            "Rewrite subjects: under 5 words, curiosity or a specific hook, personalized with {{firstName}}/{{companyName}}. A/B test two on step 1.",
            f"open {o:.1f}% vs {BENCH_OPEN:.0f}% benchmark · no mailbox issues")
    elif o < BENCH_OPEN and not mh:
        add("Subject lines", "important", f"Open rate {o:.1f}% below the {BENCH_OPEN:.0f}% benchmark",
            "Opens are under benchmark and subject lines are the primary lever.",
            "Test shorter, curiosity-driven, personalized subjects on step 1.",
            f"open {o:.1f}%")

    # 4: Worst-performing subject step (only real subjects, not thread follow-ups)
    real_subj = [s for s in steps if s.subject and not s.subject.startswith("(") and s.sent >= 30]
    if len(real_subj) >= 2:
        ranked = sorted(real_subj, key=lambda s: s.open_rate)
        worst, best = ranked[0], ranked[-1]
        if worst.open_rate > 0 and (best.open_rate - worst.open_rate) >= 8:
            add("Subject lines", "opportunity",
                f"Step {worst.step}'s subject underperforms ({worst.open_rate}% open)",
                f"“{worst.subject}” opens at {worst.open_rate}%, while step {best.step} (“{best.subject}”) opens at {best.open_rate}%. Closing that gap lifts the whole sequence.",
                f"Rewrite step {worst.step}'s subject in the style of step {best.step}.",
                f"step {worst.step}: {worst.open_rate}% vs step {best.step}: {best.open_rate}%")

    # 5: Opens but no replies (copy / CTA) at campaign level
    if o >= 25 and r < 2:
        extra = f" People even re-open ({rep}× repeat-open ratio), so the interest is real: the ask is what's missing." if rep > 1.4 else ""
        add("Email copy", "important",
            f"Strong opens ({o:.1f}%) but weak replies ({r:.1f}%)",
            f"People open but don't respond.{extra} The body and CTA aren't turning attention into replies.",
            "Cut the first email under 80 words, open with a specific observation about them (not your pitch), and end with ONE low-friction ask (“worth a quick 15 min Thursday?”).",
            f"open {o:.1f}% / reply {r:.1f}%" + (f" / repeat {rep}×" if rep > 1.4 else ""))

    # 6: A step that's opened but not replied to
    for s in steps:
        if s.sent >= 40 and s.open_rate >= 25 and s.reply_rate < 1:
            add("Email copy", "opportunity",
                f"Step {s.step} gets opened but rarely replied to",
                f"Step {s.step} opens at {s.open_rate}% but replies at just {s.reply_rate}%. It gets read but doesn't prompt action.",
                f"Rewrite step {s.step}'s body around a single, specific CTA.",
                f"step {s.step}: open {s.open_rate}% / reply {s.reply_rate}%")
            break

    # 7: Sequence structure
    if steps:
        n = len(steps)
        last = steps[-1]
        top_reply = max(steps, key=lambda s: s.unique_replies)
        if n <= 2:
            add("Sequence", "important", f"Only {n} step{'s' if n > 1 else ''} in the sequence",
                "Most cold-email replies come from follow-ups 2–4. A short sequence leaves replies on the table.",
                "Add 2–3 follow-up steps with different angles (social proof, a relevant case study, a breakup email).",
                f"{n} active steps")
        elif last.unique_replies > 0 and last.reply_rate >= 1.5:
            add("Sequence", "opportunity", f"Your last step (step {last.step}) is still converting",
                f"The final email still pulls replies ({last.unique_replies} at {last.reply_rate}%), which usually means you're stopping too early.",
                "Add one more follow-up after the current last step.",
                f"step {last.step}: {last.unique_replies} replies at {last.reply_rate}%")
        if top_reply.unique_replies > 0:
            add("Sequence", "good", f"Step {top_reply.step} drives most of your replies",
                f"Step {top_reply.step} produced {top_reply.unique_replies} replies: your strongest message.",
                f"Model your other steps and future campaigns on step {top_reply.step}'s approach.",
                f"step {top_reply.step}: {top_reply.unique_replies} replies")

    # 8: A/B testing
    vgroups = defaultdict(list)
    for s in steps:
        vgroups[s.step].append(s)
    multi = [v for v in vgroups.values() if len(v) > 1]
    if multi:
        grp = sorted(max(multi, key=lambda v: sum(x.sent for x in v)), key=lambda s: s.reply_rate, reverse=True)
        win, lose = grp[0], grp[-1]
        if win.reply_rate > lose.reply_rate:
            add("A/B testing", "opportunity", f"Variant {win.variant} is winning step {win.step}",
                f"On step {win.step}, variant {win.variant} replies at {win.reply_rate}% vs variant {lose.variant} at {lose.reply_rate}%.",
                f"Pause the losing variant and route all volume to variant {win.variant}.",
                f"variant {win.variant} {win.reply_rate}% vs {lose.variant} {lose.reply_rate}%")
    elif o < BENCH_OPEN or r < BENCH_REPLY:
        add("A/B testing", "opportunity", "No A/B test is running on this campaign",
            "With a single variant per step you can't tell what would perform better: you're optimizing blind.",
            "Add a second variant on step 1 (different subject) and let Instantly split-test it.",
            "1 variant per step")

    # 9: Targeting / unsubscribes
    if u > 2:
        add("Targeting", "important", f"High unsubscribe rate ({u:.1f}%)",
            f"{c.unsubscribed_count} people opted out. For cold outreach that's high and suggests the list or angle is off for this audience.",
            "Tighten the ICP and make the first line obviously relevant to who they are.",
            f"{c.unsubscribed_count} unsubs / {sent} = {u:.1f}%")

    # 10: Send timing
    if timing["total_opens"] >= 10 and timing["best_open_day"]:
        bd, bt = timing["best_open_day"], timing["best_open_time"]
        tr = timing["bucket_ranges"].get(bt, "")
        add("Send timing", "opportunity", f"Opens peak on {bd} ({bt})",
            f"Engagement concentrates on {bd} around {bt} ({tr}) in the campaign's local timezone. Landing in that window improves opens.",
            f"Adjust the send schedule so emails arrive {bd} morning local time.",
            f"peak {bd} {bt}")

    # 11: Warm leads to act on
    if hidden > 0:
        add("Act on warm leads", "opportunity",
            f"{hidden} warm leads opened 3+ times but never replied",
            "These prospects are clearly interested (repeated opens) but haven't responded to email: your highest-probability conversions right now.",
            "Reach out directly via LinkedIn or a call referencing your offer. Don't wait for an email reply.",
            f"{hidden} leads · 3+ opens · 0 replies")

    # 12: Scale (positive)
    if c.total_interested > 0 and r >= 2 and b < 3:
        add("Scale", "good",
            f"This campaign converts ({c.total_interested} interested, ${int(c.total_opportunity_value):,} pipeline)",
            "Engagement and conversion are healthy. Once the items above are addressed, this is a strong candidate to scale.",
            "Increase daily volume on healthy mailboxes, or clone the campaign to a similar list.",
            f"{c.total_interested} interested · ${int(c.total_opportunity_value):,}")

    recs.sort(key=lambda x: _SEV_ORDER.get(x["severity"], 9))
    health = ("critical" if any(x["severity"] == "critical" for x in recs)
              else "warning" if any(x["severity"] == "important" for x in recs) else "good")
    return {"summary": {"bottleneck": _bottleneck(c, mh), "health": health,
                        "counts": {s: sum(1 for x in recs if x["severity"] == s)
                                   for s in ("critical", "important", "opportunity", "good")}},
            "recommendations": recs}


# ── Account-wide recommendations (across all campaigns) ────────────────────────

def _has_ab(cid) -> bool:
    groups = defaultdict(int)
    for s in CampaignStep.query.filter_by(campaign_id=cid).all():
        groups[s.step] += 1
    return any(v > 1 for v in groups.values())


def account_recommendations(account_id=None) -> Dict:
    """Same intelligence as per-campaign, aggregated across every campaign:
    the systemic bottleneck, a fix/scale/review triage, and cross-campaign learnings."""
    cq = Campaign.query
    if account_id:
        cq = cq.filter(Campaign.account_id == account_id)
    active = cq.filter(Campaign.emails_sent_count >= 30).all()
    empty = {"summary": {"bottleneck": {"stage": "Data", "verdict": "Not enough campaign data yet."},
                         "health": "unknown", "counts": {}, "n_campaigns": 0},
             "recommendations": [], "triage": {"fix_first": [], "scale": [], "review": []}}
    if not active:
        return empty

    t = _campaign_totals(account_id)
    acct_open = _pct(t["opens_unique"], t["contacted"])
    acct_reply = _pct(t["replies_unique"], t["contacted"])
    deliver = deliverability_summary(account_id)
    n = len(active)

    # tally per-campaign bottlenecks
    btl = Counter()
    binfo = {}
    for c in active:
        stage = _bottleneck(c, _campaign_mailboxes(c))["stage"]
        binfo[c.id] = stage
        btl[stage] += 1

    recs = []

    def add(cat, sev, title, detail, action, evidence):
        recs.append({"category": cat, "severity": sev, "color": _SEV_COLOR[sev],
                     "title": title, "detail": detail, "action": action, "evidence": evidence})

    # systemic bottleneck → verdict
    problem = [(s, c) for s, c in btl.most_common() if s != "Scaling"]
    systemic = problem[0][0] if problem else "Scaling"
    if systemic == "Deliverability":
        verdict = {"stage": "Deliverability", "verdict": f"Deliverability is the recurring problem across your account ({btl['Deliverability']} of {n} campaigns). Fix sender health before optimizing copy anywhere."}
    elif systemic == "Open rate":
        verdict = {"stage": "Subject lines", "verdict": f"Opens are the recurring bottleneck ({btl['Open rate']} of {n} campaigns; account open rate {acct_open}%). Subject lines are your highest-leverage fix account-wide."}
    elif systemic == "Reply rate":
        verdict = {"stage": "Email copy", "verdict": f"Replies are the recurring bottleneck ({btl['Reply rate']} of {n} campaigns; account reply rate {acct_reply}%). Your email copy and CTA need work across the board."}
    elif systemic == "Conversion":
        verdict = {"stage": "Conversion", "verdict": "People engage but conversion to interested is the recurring gap: revisit your offer and targeting fit."}
    else:
        verdict = {"stage": "Scaling", "verdict": f"Your campaigns are healthy on average (open {acct_open}%, reply {acct_reply}%). The account-wide lever now is volume: scale your winners."}

    # account deliverability
    if deliver["issues"] > 0:
        has_err = any(a.health_state == "error" for a in deliver["accounts"])
        add("Deliverability", "critical" if has_err else "important",
            f"{deliver['issues']} of {deliver['total']} mailboxes need attention",
            "Some sending mailboxes are disconnected or landing in spam, which drags down opens on every campaign they touch.",
            "Open the Deliverability page to see which, then reconnect or pause them.",
            f"{deliver['issues']} mailbox issue(s) of {deliver['total']}")
    high_bounce = [c for c in active if c.bounce_rate > 5]
    if high_bounce:
        add("Deliverability", "critical" if len(high_bounce) > 1 else "important",
            f"{len(high_bounce)} campaign(s) bounce above 5%",
            f"High bounce on {', '.join(c.name for c in high_bounce[:3])} damages the reputation of shared mailboxes: which hurts your other campaigns too.",
            "Verify and clean those lists before sending more from the same mailboxes.",
            f"{len(high_bounce)} campaigns >5% bounce")

    # systemic subject
    if systemic == "Open rate":
        add("Subject lines", "important", f"Subject lines are a systemic weakness (account open {acct_open}%)",
            f"{btl['Open rate']} campaigns are bottlenecked on opens: the same lever everywhere, so a focused effort compounds.",
            "Standardize on a proven subject formula and A/B test it across your campaigns.",
            f"{btl['Open rate']}/{n} campaigns · open {acct_open}%")

    # systemic copy
    copy_weak = [c for c in active if c.open_rate >= 25 and c.reply_rate < 2]
    if len(copy_weak) >= 2:
        add("Email copy", "important", f"{len(copy_weak)} campaigns get opens but few replies",
            f"{', '.join(c.name for c in copy_weak[:3])} open well but convert poorly to replies: a systemic copy/CTA problem, not a subject one.",
            "Rework your first-email template: short, value-first, one clear CTA: then roll it across these campaigns.",
            f"{len(copy_weak)} campaigns: open≥25% / reply<2%")

    # best subject to replicate
    acct_cids = {c.id for c in active}
    step_q = CampaignStep.query.filter(CampaignStep.sent >= 40)
    if acct_cids:
        step_q = step_q.filter(CampaignStep.campaign_id.in_(acct_cids))
    subj_steps = [s for s in step_q.all()
                  if s.subject and not s.subject.startswith("(")]
    if subj_steps:
        bs = max(subj_steps, key=lambda s: s.open_rate)
        if bs.open_rate >= 25:
            bsc = db.session.get(Campaign, bs.campaign_id)
            add("Replicate winners", "good", f"Your best subject opens at {bs.open_rate}%",
                f"“{bs.subject}” (in {bsc.name if bsc else ''}) is your top opener, well above the account average ({acct_open}%).",
                "Use this subject's style as the template for your underperforming campaigns.",
                f"“{bs.subject[:42]}” {bs.open_rate}%")

    # best replying step
    rep_steps = step_q.all()
    if rep_steps:
        br = max(rep_steps, key=lambda s: s.reply_rate)
        if br.reply_rate >= 2:
            brc = db.session.get(Campaign, br.campaign_id)
            add("Replicate winners", "good", f"Your best-replying message converts at {br.reply_rate}%",
                f"Step {br.step} in {brc.name if brc else ''} replies at {br.reply_rate}% vs your account average of {acct_reply}%.",
                "Study that message's angle and structure, and reuse it in other campaigns.",
                f"step {br.step}: {br.reply_rate}% reply")

    # best campaign to scale
    scalers = [c for c in active if c.total_interested > 0]
    if scalers:
        bc = max(scalers, key=lambda c: (c.total_interested, c.total_opportunity_value))
        add("Scale", "good", f"{bc.name} is your top converter",
            f"{bc.total_interested} interested and ${int(bc.total_opportunity_value):,} pipeline: your best-performing campaign.",
            "Increase its daily volume on healthy mailboxes, or clone the list/approach to a similar audience.",
            f"{bc.total_interested} interested · ${int(bc.total_opportunity_value):,}")

    # A/B systemic
    no_ab = [c for c in active if not _has_ab(c.id)]
    if len(no_ab) >= 2:
        add("A/B testing", "opportunity", f"{len(no_ab)} of {n} campaigns run no A/B test",
            "Without variants you can't learn what works: and testing across the account compounds learning fast.",
            "Add a second subject variant on step 1 of your highest-volume campaigns.",
            f"{len(no_ab)} campaigns, single variant")

    # warm leads account-wide
    hq = Prospect.query.filter(Prospect.email_open_count >= WARM_THRESHOLD,
                               Prospect.email_reply_count == 0)
    if account_id:
        hq = hq.filter(Prospect.account_id == account_id)
    total_hidden = hq.count()
    if total_hidden > 0:
        add("Act on warm leads", "opportunity", f"{total_hidden} warm leads across the account never replied",
            "They opened 3+ times but didn't respond: your largest pool of high-intent, unworked prospects.",
            "Work them directly via LinkedIn or call. The Hidden Intent list has them ranked.",
            f"{total_hidden} leads · 3+ opens · 0 replies")

    # timing account-wide
    timing = engagement_timing(account_id=account_id)
    if timing["total_opens"] >= 20 and timing["best_open_day"]:
        add("Send timing", "opportunity", f"Account-wide, opens peak on {timing['best_open_day']} ({timing['best_open_time']})",
            "Engagement concentrates in this window across campaigns, each in its own local timezone.",
            "Bias your send schedules toward this day and time.",
            f"peak {timing['best_open_day']} {timing['best_open_time']}")

    # ── triage: fix / scale / review (mutually exclusive) ──
    fix_first, scale_list, review = [], [], []
    used = set()
    cand = sorted([c for c in active if binfo[c.id] == "Deliverability" or c.open_rate < 15 or c.bounce_rate > 5],
                  key=lambda c: c.emails_sent_count, reverse=True)
    for c in cand[:4]:
        fix_first.append({"c": c, "reason": f"{binfo[c.id].lower()} · open {c.open_rate}%"})
        used.add(c.id)
    for c in sorted([c for c in active if c.id not in used and c.total_interested > 0
                     and c.reply_rate >= 2 and c.bounce_rate < 3],
                    key=lambda c: -c.total_interested)[:4]:
        scale_list.append({"c": c, "reason": f"{c.total_interested} interested · ${int(c.total_opportunity_value):,}"})
        used.add(c.id)
    for c in sorted([c for c in active if c.id not in used and c.emails_sent_count >= 100
                     and c.total_interested == 0 and c.reply_rate < 1],
                    key=lambda c: -c.emails_sent_count)[:4]:
        review.append({"c": c, "reason": f"{c.emails_sent_count} sent · 0 interested"})
        used.add(c.id)

    recs.sort(key=lambda x: _SEV_ORDER.get(x["severity"], 9))
    health = ("critical" if any(x["severity"] == "critical" for x in recs)
              else "warning" if any(x["severity"] == "important" for x in recs) else "good")
    return {"summary": {"bottleneck": verdict, "health": health,
                        "counts": {s: sum(1 for x in recs if x["severity"] == s)
                                   for s in ("critical", "important", "opportunity", "good")},
                        "acct_open": acct_open, "acct_reply": acct_reply, "n_campaigns": n},
            "recommendations": recs,
            "triage": {"fix_first": fix_first, "scale": scale_list, "review": review}}


# ── Trends ─────────────────────────────────────────────────────────────────────

def daily_series(campaign_id: Optional[str] = None, days: Optional[int] = None,
                 start: Optional[date] = None, end: Optional[date] = None,
                 account_id=None) -> List[Dict]:
    if start is None and days:
        start = date.today() - timedelta(days=days)
    q = db.session.query(
        DailyMetric.date, func.sum(DailyMetric.sent), func.sum(DailyMetric.contacted),
        func.sum(DailyMetric.opened), func.sum(DailyMetric.unique_opened),
        func.sum(DailyMetric.replies), func.sum(DailyMetric.unique_replies),
    )
    if start:
        q = q.filter(DailyMetric.date >= start.isoformat())
    if end:
        q = q.filter(DailyMetric.date <= end.isoformat())
    if campaign_id:
        q = q.filter(DailyMetric.campaign_id == campaign_id)
    elif account_id:
        cids = _acct_campaign_ids(account_id)
        if not cids:
            return []
        q = q.filter(DailyMetric.campaign_id.in_(cids))
    q = q.group_by(DailyMetric.date).order_by(DailyMetric.date)
    out = []
    for d, sent, contacted, opened, uopen, replies, ureplies in q.all():
        out.append({"date": d, "sent": int(sent or 0), "contacted": int(contacted or 0),
                    "opened": int(uopen or 0), "replies": int(ureplies or 0),
                    "open_rate": _pct(uopen or 0, contacted or 0),
                    "reply_rate": _pct(ureplies or 0, contacted or 0)})
    return out


def period_comparison(days: int = 7) -> Dict:
    def window(start, end):
        rows = db.session.query(
            func.sum(DailyMetric.contacted), func.sum(DailyMetric.unique_opened),
            func.sum(DailyMetric.unique_replies),
        ).filter(DailyMetric.date >= start, DailyMetric.date < end).first()
        contacted, opened, replies = (rows or (0, 0, 0))
        return {"contacted": int(contacted or 0), "open_rate": _pct(opened or 0, contacted or 0),
                "reply_rate": _pct(replies or 0, contacted or 0)}
    today = date.today()
    cur = window((today - timedelta(days=days)).isoformat(), (today + timedelta(days=1)).isoformat())
    prev = window((today - timedelta(days=days * 2)).isoformat(), (today - timedelta(days=days)).isoformat())
    return {"current": cur, "previous": prev,
            "delta_open": round(cur["open_rate"] - prev["open_rate"], 1),
            "delta_reply": round(cur["reply_rate"] - prev["reply_rate"], 1),
            "delta_contacted": cur["contacted"] - prev["contacted"], "days": days}


# ── Script / A-B analysis ──────────────────────────────────────────────────────

def step_analysis(campaign_id: Optional[str] = None) -> List[Dict]:
    q = CampaignStep.query.filter(CampaignStep.sent > 0)
    if campaign_id:
        q = q.filter_by(campaign_id=campaign_id)
    rows = []
    for s in q.all():
        c = db.session.get(Campaign, s.campaign_id)
        rows.append({"campaign_id": s.campaign_id, "campaign_name": c.name if c else s.campaign_id,
                     "step": s.step, "variant": s.variant, "subject": s.subject, "sent": s.sent,
                     "open_rate": s.open_rate, "reply_rate": s.reply_rate,
                     "unique_opened": s.unique_opened, "unique_replies": s.unique_replies,
                     "health": _step_health(s)})
    rows.sort(key=lambda r: (r["reply_rate"], r["open_rate"]), reverse=True)
    return rows


def _step_health(s: CampaignStep) -> str:
    if s.reply_rate >= 4:
        return "good"
    if s.open_rate >= 25 and s.reply_rate >= 2:
        return "warning"
    if s.open_rate < 15:
        return "critical"
    return "warning"


def sequence_funnel(campaign_id: str) -> List[Dict]:
    steps = (CampaignStep.query.filter_by(campaign_id=campaign_id)
             .filter(CampaignStep.sent > 0).order_by(CampaignStep.step).all())
    return [{"step": s.step, "subject": s.subject, "sent": s.sent,
             "unique_opened": s.unique_opened, "unique_replies": s.unique_replies,
             "open_rate": s.open_rate, "reply_rate": s.reply_rate} for s in steps]


def ab_variants(campaign_id: Optional[str] = None) -> List[Dict]:
    q = CampaignStep.query.filter(CampaignStep.sent > 0)
    if campaign_id:
        q = q.filter_by(campaign_id=campaign_id)
    groups = defaultdict(list)
    for s in q.all():
        groups[(s.campaign_id, s.step)].append(s)
    out = []
    for (cid, step), variants in groups.items():
        if len(variants) < 2:
            continue
        c = db.session.get(Campaign, cid)
        variants.sort(key=lambda v: v.reply_rate, reverse=True)
        out.append({"campaign_name": c.name if c else cid, "step": step,
                    "variants": [{"variant": v.variant, "subject": v.subject, "sent": v.sent,
                                  "open_rate": v.open_rate, "reply_rate": v.reply_rate} for v in variants],
                    "winner": variants[0].variant})
    return out


# ── Deliverability ─────────────────────────────────────────────────────────────

def deliverability_summary(account_id=None) -> Dict:
    if account_id:
        accounts = SendingAccount.query.filter_by(account_id=account_id).all()
    else:
        accounts = SendingAccount.query.all()
    if not accounts:
        return {"accounts": [], "total": 0, "issues": 0, "healthy": 0, "warming": 0,
                "total_inbox": 0, "total_spam": 0, "inbox_rate": 0}
    issues = sum(1 for a in accounts if a.is_issue)
    warming = sum(1 for a in accounts if a.has_placement_data)
    total_inbox = sum(a.wa_landed_inbox for a in accounts)
    total_spam = sum(a.wa_landed_spam for a in accounts)
    order = {"error": 0, "spam": 1, "healthy": 2, "ok": 3, "paused": 4}
    return {"accounts": sorted(accounts, key=lambda a: (order[a.health_state], -a.warmup_score)),
            "total": len(accounts), "issues": issues, "healthy": len(accounts) - issues,
            "warming": warming, "total_inbox": total_inbox, "total_spam": total_spam,
            "inbox_rate": _pct(total_inbox, total_inbox + total_spam)}


# ── Revenue + sentiment ────────────────────────────────────────────────────────

def revenue_summary(account_id=None) -> Dict:
    t = _campaign_totals(account_id)
    cq = Campaign.query.filter(Campaign.total_opportunity_value > 0)
    if account_id:
        cq = cq.filter(Campaign.account_id == account_id)
    campaigns = cq.order_by(Campaign.total_opportunity_value.desc()).all()
    contacted = int(t["contacted"]) or 1
    return {"pipeline_value": int(t["opp_value"]), "opportunities": int(t["opportunities"]),
            "interested": int(t["interested"]), "meetings": int(t["meetings"]), "closed": int(t["closed"]),
            "value_per_opp": round(t["opp_value"] / t["opportunities"]) if t["opportunities"] else 0,
            "opp_rate": _pct(t["opportunities"], contacted), "campaigns": campaigns}


def sentiment_breakdown(account_id=None) -> List[Dict]:
    q = (db.session.query(Prospect.interest_label, func.count(Prospect.id))
         .filter(Prospect.interest_label.isnot(None)))
    if account_id:
        q = q.filter(Prospect.account_id == account_id)
    rows = q.group_by(Prospect.interest_label).all()
    palette = {"Interested": "#0a8f5b", "Meeting Booked": "#ea580c", "Meeting Completed": "#e08a1e",
               "Closed": "#0a8f5b", "Out of Office": "#969cab", "Not Interested": "#d63b3b",
               "Wrong Person": "#d63b3b", "Lost": "#d63b3b"}
    return [{"label": lbl, "count": cnt, "color": palette.get(lbl, "#6366f1")}
            for lbl, cnt in sorted(rows, key=lambda x: -x[1])]


# ── HeyReach (LinkedIn) analytics ─────────────────────────────────────────────

def heyreach_kpis(account_id: int) -> Dict:
    """Top-level LinkedIn KPIs for the dashboard."""
    campaigns = HeyReachCampaign.query.filter_by(account_id=account_id).all()
    # leads_contacted = people who actually started the sequence (in_progress + finished + failed)
    # total_leads includes queued-but-not-yet-started; exclude those
    leads_contacted = sum((c.leads_in_progress or 0) + (c.leads_finished or 0) + (c.leads_failed or 0) for c in campaigns)
    total_finished  = sum(c.leads_finished or 0 for c in campaigns)

    lq = HeyReachLead.query.filter_by(account_id=account_id)
    total_leads    = lq.count()
    interested     = lq.filter_by(tag_interested=True).count()
    not_interested = lq.filter_by(tag_not_interested=True).count()
    generic        = lq.filter_by(tag_generic=True).count()
    any_response   = interested + not_interested + generic

    # Rate denominators use leads_contacted (actually messaged), not total_leads (DB count)
    return {
        "campaigns":       len(campaigns),
        "total_sent":      leads_contacted,
        "total_finished":  total_finished,
        "completion_rate": _pct(total_finished, leads_contacted),
        "total_leads":     total_leads,
        "interested":      interested,
        "not_interested":  not_interested,
        "generic":         generic,
        "any_response":    any_response,
        "interest_rate":   _pct(interested, leads_contacted),
        "response_rate":   _pct(any_response, leads_contacted),
    }


def heyreach_campaigns(account_id: int) -> List:
    return (HeyReachCampaign.query
            .filter_by(account_id=account_id)
            .order_by(HeyReachCampaign.created_at.desc())
            .all())


def _norm(s: str) -> str:
    import re as _re
    return _re.sub(r'[^a-z0-9]', '', (s or '').lower())


def cross_channel_leads(account_id: int, limit: int = 50) -> List[Dict]:
    """
    Match HeyReach LinkedIn leads to Instantly email prospects by
    normalised (first+last name, company). Score each matched lead
    on multi-channel engagement signals and return sorted by score desc.

    Score weights:
        +2  matched on BOTH channels (base multi-channel bonus)
        +1  email opened 1-2x
        +2  email opened 3-4x
        +3  email opened 5+x
        +3  email replied
        +4  email disposition = interested
        +5  email stage = Meeting / Won
        +3  LinkedIn tag = Interested
        -3  LinkedIn tag = Not Interested
    Tiers: Hot ≥ 7 | Warm 3-6 | Contacted 1-2
    """
    # Pull all HeyReach leads for this account
    hr_leads = HeyReachLead.query.filter_by(account_id=account_id).all()
    if not hr_leads:
        return []

    # Build lookup: (norm_name, norm_company) → HeyReachLead
    hr_map: dict[tuple, HeyReachLead] = {}
    for hl in hr_leads:
        key = (_norm(hl.first_name) + _norm(hl.last_name), _norm(hl.company_name))
        hr_map[key] = hl   # last-write wins for dupes

    # Pull all Instantly prospects for this account
    prospects = Prospect.query.filter_by(account_id=account_id).all()

    results = []

    # Track which HeyReach leads were matched (to include unmatched LinkedIn-only leads)
    matched_hr_ids = set()

    for p in prospects:
        key = (_norm(p.first_name or '') + _norm(p.last_name or ''), _norm(p.company_name or ''))
        hr_lead = hr_map.get(key)

        score = 0
        signals = []

        # Email signals
        opens = p.email_open_count or 0
        if opens >= 5:
            score += 3; signals.append(f"Opened {opens}x")
        elif opens >= 3:
            score += 2; signals.append(f"Opened {opens}x")
        elif opens >= 1:
            score += 1; signals.append(f"Opened {opens}x")

        if p.email_reply_count and p.email_reply_count > 0:
            score += 3; signals.append("Replied to email")

        if p.disposition == "interested":
            score += 4; signals.append("Email: Interested")

        if p.stage in ("Meeting", "Won"):
            score += 5; signals.append(f"Stage: {p.stage}")

        if hr_lead:
            matched_hr_ids.add(hr_lead.id)
            score += 2   # multi-channel bonus
            signals.append("On LinkedIn")

            if hr_lead.tag_interested:
                score += 3; signals.append("LinkedIn: Interested")
            if hr_lead.tag_not_interested:
                score -= 3; signals.append("LinkedIn: Not interested")

        if score <= 0:
            continue

        tier = "Hot" if score >= 7 else "Warm" if score >= 3 else "Contacted"

        results.append({
            "score": score,
            "tier": tier,
            "signals": signals,
            "prospect": p,
            "hr_lead": hr_lead,
            "on_both": hr_lead is not None,
        })

    # Also include HeyReach-only leads that are Interested but not matched
    for hl in hr_leads:
        if hl.id in matched_hr_ids:
            continue
        if not hl.tag_interested:
            continue
        score = 3   # LinkedIn Interested baseline
        results.append({
            "score": score,
            "tier": "Warm",
            "signals": ["LinkedIn: Interested", "LinkedIn only"],
            "prospect": None,
            "hr_lead": hl,
            "on_both": False,
        })

    results.sort(key=lambda x: -x["score"])
    return results[:limit]


# ── LinkedIn lead pages ────────────────────────────────────────────────────────

def linkedin_leads_filtered(account_id, campaign_id=None, tag=None, search=None, limit=500):
    """Filtered + sorted HeyReach lead list for the LinkedIn leads page."""
    q = HeyReachLead.query.filter_by(account_id=account_id)
    if campaign_id:
        q = q.filter(HeyReachLead.campaign_id == campaign_id)
    if tag == "interested":
        q = q.filter(HeyReachLead.tag_interested == True)
    elif tag == "not_interested":
        q = q.filter(HeyReachLead.tag_not_interested == True)
    elif tag == "generic":
        q = q.filter(HeyReachLead.tag_generic == True)
    elif tag == "untagged":
        q = q.filter(
            HeyReachLead.tag_interested == False,
            HeyReachLead.tag_not_interested == False,
            HeyReachLead.tag_generic == False,
        )
    if search:
        s = f"%{search}%"
        q = q.filter(db.or_(
            HeyReachLead.first_name.ilike(s),
            HeyReachLead.last_name.ilike(s),
            HeyReachLead.company_name.ilike(s),
            HeyReachLead.headline.ilike(s),
            HeyReachLead.position.ilike(s),
        ))
    return q.order_by(
        HeyReachLead.tag_interested.desc(),
        HeyReachLead.tag_generic.desc(),
        HeyReachLead.first_name.asc(),
    ).limit(limit).all()


def linkedin_cross_match(account_id, leads):
    """Return {lead.id: Prospect} for any LinkedIn leads that match an email prospect."""
    prospects = Prospect.query.filter_by(account_id=account_id).all()
    prospect_map = {}
    for p in prospects:
        key = (
            _norm(p.first_name or "") + _norm(p.last_name or ""),
            _norm(p.company_name or p.company_domain or ""),
        )
        if key[0] or key[1]:
            prospect_map[key] = p
    matched = {}
    for lead in leads:
        key = (
            _norm(lead.first_name or "") + _norm(lead.last_name or ""),
            _norm(lead.company_name or ""),
        )
        if key in prospect_map:
            matched[lead.id] = prospect_map[key]
    return matched


def linkedin_audience_insights(account_id, campaign_id=None):
    """Top companies, job titles, and locations from HeyReach leads."""
    q = HeyReachLead.query.filter_by(account_id=account_id)
    if campaign_id:
        q = q.filter(HeyReachLead.campaign_id == campaign_id)
    leads = q.all()
    companies: Counter = Counter()
    titles: Counter = Counter()
    locations: Counter = Counter()
    for lead in leads:
        if lead.company_name and lead.company_name.strip():
            companies[lead.company_name.strip()] += 1
        if lead.position and lead.position.strip():
            titles[lead.position.strip()] += 1
        if lead.location and lead.location.strip():
            city = lead.location.split(",")[0].strip()
            if city:
                locations[city] += 1
    return {
        "top_companies": companies.most_common(8),
        "top_titles": titles.most_common(8),
        "top_locations": locations.most_common(6),
    }


# ── Sprint 2: Combined Channel Intelligence ────────────────────────────────────

def _cross_match_keys(account_id: int):
    """Return (hr_key_set, prospect_key_set) — normalised (name, company) tuples."""
    hr_keys = set()
    for hl in HeyReachLead.query.filter_by(account_id=account_id).all():
        hr_keys.add((_norm(hl.first_name) + _norm(hl.last_name), _norm(hl.company_name)))
    p_keys = set()
    for p in Prospect.query.filter_by(account_id=account_id).all():
        p_keys.add((_norm(p.first_name or '') + _norm(p.last_name or ''), _norm(p.company_name or '')))
    return hr_keys, p_keys


def unified_funnel(account_id: int) -> List[Dict]:
    """Single funnel aggregating both email and LinkedIn channels."""
    camps    = Campaign.query.filter_by(account_id=account_id).all()
    li_camps = HeyReachCampaign.query.filter_by(account_id=account_id).all()

    # ── email ──
    email_audience  = Prospect.query.filter_by(account_id=account_id).count()
    # contacted_count is Instantly's own "unique leads contacted" per campaign
    email_contacted = sum(c.contacted_count or 0 for c in camps)
    email_opens     = sum(c.open_count_unique or 0 for c in camps)
    email_replies   = sum(c.reply_count_unique or 0 for c in camps)
    email_interest  = sum(c.total_interested or 0 for c in camps)
    email_meetings  = sum(c.total_meeting_booked or 0 for c in camps)

    # ── linkedin ──
    li_q           = HeyReachLead.query.filter_by(account_id=account_id)
    li_audience    = li_q.count()
    # contacted = leads who actually started the sequence (not just queued)
    li_contacted   = sum((c.leads_in_progress or 0) + (c.leads_finished or 0) + (c.leads_failed or 0) for c in li_camps)
    li_interested  = li_q.filter_by(tag_interested=True).count()
    li_generic     = li_q.filter_by(tag_generic=True).count()
    li_not_int     = li_q.filter_by(tag_not_interested=True).count()
    # any_response = all LinkedIn replies regardless of sentiment (the only engagement signal HeyReach gives us)
    li_any_resp    = li_interested + li_generic + li_not_int
    li_meetings    = (HeyReachLead.query.filter_by(account_id=account_id, li_stage='Meeting').count() +
                      HeyReachLead.query.filter_by(account_id=account_id, li_stage='Won').count())

    # ── deduplication ──
    hr_keys, p_keys = _cross_match_keys(account_id)
    cross = len(hr_keys & p_keys)

    # ── combined stages ──
    # Audience: unique people across both channels
    audience   = email_audience + li_audience - cross
    # Contacted: cap email side at email_audience (contacted_count can exceed unique prospects
    # if the same person appears in multiple campaigns)
    email_contacted = min(email_contacted, email_audience)
    contacted  = email_contacted + li_contacted
    # Engaged: email opens (intent signal) + any LinkedIn reply (only engagement signal available from HeyReach)
    engaged    = email_opens + li_any_resp
    # Responded: email replies + positive/neutral LinkedIn responses (excluding explicit not_interested)
    responded  = email_replies + li_interested + li_generic
    # Interested: clear positive intent on either channel
    interested = email_interest + li_interested
    # Meeting: booked across both
    meeting    = email_meetings + li_meetings

    base = max(audience, 1)
    stages = [
        ("Audience",   audience,   f"{email_audience:,} email, {li_audience:,} LinkedIn",          "#5b54f0"),
        ("Contacted",  contacted,  f"{email_contacted:,} emailed, {li_contacted:,} LinkedIn",       "#7c75f3"),
        ("Engaged",    engaged,    f"{email_opens:,} email opens, {li_any_resp:,} LinkedIn replies","#7c3aed"),
        ("Responded",  responded,  f"{email_replies:,} email replies, {li_interested+li_generic:,} LinkedIn positive", "#0d9488"),
        ("Interested", interested, f"{email_interest:,} email, {li_interested:,} LinkedIn",         "#0a8f5b"),
        ("Meeting",    meeting,    f"{email_meetings:,} email, {li_meetings:,} LinkedIn",            "#e26a1b"),
    ]
    return [{"label": l, "value": v, "detail": d, "color": c,
             "pct": round(v / base * 100, 1)} for l, v, d, c in stages]


def channel_lift(account_id: int) -> Dict:
    """Compare response rates: email-only vs LinkedIn-only vs both channels."""
    hr_all  = HeyReachLead.query.filter_by(account_id=account_id).all()
    p_all   = Prospect.query.filter_by(account_id=account_id).all()
    if not hr_all or not p_all:
        return {"has_data": False}

    # Build lookup maps
    hr_map: dict = {}
    for hl in hr_all:
        k = (_norm(hl.first_name) + _norm(hl.last_name), _norm(hl.company_name))
        hr_map[k] = hl

    p_map: dict = {}
    for p in p_all:
        k = (_norm(p.first_name or '') + _norm(p.last_name or ''), _norm(p.company_name or ''))
        p_map[k] = p

    email_only_total = email_only_resp = 0
    both_total       = both_resp       = 0

    for p in p_all:
        k = (_norm(p.first_name or '') + _norm(p.last_name or ''), _norm(p.company_name or ''))
        hl = hr_map.get(k)
        responded = (p.email_reply_count or 0) > 0 or p.disposition == 'interested'
        if hl:
            both_total += 1
            if responded or hl.tag_interested or hl.tag_generic:
                both_resp += 1
        else:
            email_only_total += 1
            if responded:
                email_only_resp += 1

    li_only_total = li_only_resp = 0
    for hl in hr_all:
        k = (_norm(hl.first_name) + _norm(hl.last_name), _norm(hl.company_name))
        if k not in p_map:
            li_only_total += 1
            if hl.tag_interested or hl.tag_generic:
                li_only_resp += 1

    LIFT_MIN_SAMPLE = 20  # need at least this many cross-channel leads for a reliable rate
    if both_total < LIFT_MIN_SAMPLE:
        return {
            "has_data": False,
            "reason": "insufficient_data",
            "both_total": both_total,
            "needed": LIFT_MIN_SAMPLE,
        }

    er = _pct(email_only_resp, email_only_total)
    lr = _pct(li_only_resp, li_only_total)
    br = _pct(both_resp, both_total)
    best_single = max(er, lr, 0.01)
    lift = round(br / best_single, 1)

    return {
        "has_data":   True,
        "email_only": {"total": email_only_total, "responded": email_only_resp, "rate": er},
        "li_only":    {"total": li_only_total,    "responded": li_only_resp,    "rate": lr},
        "both":       {"total": both_total,        "responded": both_resp,       "rate": br},
        "lift":       lift,
    }


def gap_lists(account_id: int, limit: int = 25) -> Dict:
    """
    Two actionable lists:
      email_warm_not_li  — warm email leads (opened 3+) not yet on LinkedIn
      li_interested_not_email — LinkedIn interested leads never emailed
    """
    hr_all = HeyReachLead.query.filter_by(account_id=account_id).all()
    p_all  = Prospect.query.filter_by(account_id=account_id).all()

    hr_keys = {(_norm(hl.first_name) + _norm(hl.last_name), _norm(hl.company_name)) for hl in hr_all}
    p_keys  = {(_norm(p.first_name or '') + _norm(p.last_name or ''), _norm(p.company_name or '')) for p in p_all}

    warm_not_li = [
        p for p in p_all
        if (p.warm_score or 0) >= WARM_THRESHOLD
        and (_norm(p.first_name or '') + _norm(p.last_name or ''), _norm(p.company_name or '')) not in hr_keys
    ]
    warm_not_li.sort(key=lambda p: -(p.warm_score or 0))

    li_int_not_email = [
        hl for hl in hr_all
        if hl.tag_interested
        and (_norm(hl.first_name) + _norm(hl.last_name), _norm(hl.company_name)) not in p_keys
    ]

    return {
        "email_warm_not_li":       warm_not_li[:limit],
        "li_interested_not_email": li_int_not_email[:limit],
    }


def best_send_time(account_id: int) -> Dict:
    """
    Day-of-week and hour-of-day breakdown for email_opened events.
    Returns counts used to render a heatmap.
    """
    DAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    events = (Event.query
              .filter_by(account_id=account_id, type='email_opened')
              .filter(Event.occurred_at.isnot(None))
              .all())

    day_counts  = {d: 0 for d in DAYS}
    hour_counts = {h: 0 for h in range(24)}
    heatmap     = {d: {h: 0 for h in range(24)} for d in DAYS}

    for e in events:
        d = DAYS[e.occurred_at.weekday()]
        h = e.occurred_at.hour
        day_counts[d]  += 1
        hour_counts[h] += 1
        heatmap[d][h]  += 1

    best_day  = max(day_counts,  key=day_counts.get)  if any(day_counts.values())  else None
    best_hour = max(hour_counts, key=hour_counts.get) if any(hour_counts.values()) else None

    return {
        "days":        DAYS,
        "day_counts":  day_counts,
        "hour_counts": hour_counts,
        "heatmap":     heatmap,
        "best_day":    best_day,
        "best_hour":   best_hour,
        "total":       len(events),
    }


# ── Sprint 3: Lead Score, Next Best Action, ICP Learner, Lead Exhaustion ──────

def _build_hr_map(account_id: int) -> dict:
    """Build {(norm_name, norm_co): HeyReachLead} for cross-channel matching."""
    hr_map = {}
    for hl in HeyReachLead.query.filter_by(account_id=account_id).all():
        k = (_norm(hl.first_name) + _norm(hl.last_name), _norm(hl.company_name))
        hr_map[k] = hl
    return hr_map


def compute_lead_score(p: Prospect, hr_lead=None) -> int:
    """
    0-100 signal score: best intent tier x recency multiplier.
    Uses only the strongest signal (no stacking) to prevent gaming by bot opens.
    """
    opens   = p.email_open_count or 0
    replies = p.email_reply_count or 0
    clicks  = p.email_click_count or 0
    code    = p.lt_interest_status

    # Intent tier: highest signal wins
    if code in (2, 3):                               intent = 100  # meeting booked/completed
    elif code == 1:                                  intent = 80   # Instantly Interested label
    elif hr_lead and hr_lead.tag_interested:         intent = 70   # LinkedIn interested
    elif replies > 0:                                intent = 50   # email replied
    elif hr_lead and hr_lead.tag_generic:            intent = 35   # LinkedIn any reply
    elif opens >= 3:                                 intent = 20   # warm (3+ opens)
    elif clicks > 0:                                 intent = 15   # clicked link
    elif opens >= 1:                                 intent = 8    # cold open
    else:                                            intent = 0

    if intent == 0:
        return 0

    # Recency: days since last meaningful activity
    last = (p.timestamp_last_reply or p.timestamp_last_open
            or p.timestamp_last_contact or p.last_activity_at)
    days = (datetime.utcnow() - last).days if last else 999

    if   days <= 3:   recency = 1.00
    elif days <= 7:   recency = 0.95
    elif days <= 14:  recency = 0.85
    elif days <= 30:  recency = 0.70
    elif days <= 60:  recency = 0.50
    elif days <= 90:  recency = 0.35
    else:             recency = 0.20

    score = round(intent * recency)

    if hr_lead:                            score = min(100, score + 8)   # cross-channel bonus
    if hr_lead and hr_lead.tag_not_interested:  score = max(0, score - 20)  # negative signal

    return min(100, max(0, score))


def lead_scores(account_id: int) -> dict:
    """Return {prospect_id: score} for all prospects. Two DB queries total."""
    prospects = Prospect.query.filter_by(account_id=account_id).all()
    hr_map    = _build_hr_map(account_id)
    out = {}
    for p in prospects:
        k = (_norm(p.first_name or '') + _norm(p.last_name or ''), _norm(p.company_name or ''))
        out[p.id] = compute_lead_score(p, hr_map.get(k))
    return out


def _next_action(p: Prospect, hr_lead) -> Optional[Dict]:
    """Single-lead deterministic next action. Returns {label, urgency} or None."""
    now     = datetime.utcnow()
    replies = p.email_reply_count or 0
    opens   = p.email_open_count or 0
    code    = p.lt_interest_status
    is_on_li = hr_lead is not None

    if p.stage in ("Won", "Lost"):       return None
    if p.stage == "Meeting":             return None
    if code in (2, 3):                   return None   # meeting booked/completed

    # Reply going cold
    if replies > 0 and p.timestamp_last_reply:
        if (now - p.timestamp_last_reply).days >= 7:
            return {"label": "Follow up in thread", "urgency": "high"}

    # LinkedIn interested
    if hr_lead and hr_lead.tag_interested:
        if code == 1 or replies > 0:
            return {"label": "Schedule call, interested on both", "urgency": "high"}
        return {"label": "Interested on LinkedIn, move to meeting", "urgency": "high"}

    # Email replied, not on LinkedIn yet
    if replies > 0 and not is_on_li:
        return {"label": "Connect on LinkedIn", "urgency": "medium"}

    # 3+ opens, no reply, not on LinkedIn
    if opens >= 3 and replies == 0 and not is_on_li:
        return {"label": "Opening repeatedly, try LinkedIn", "urgency": "medium"}

    # 3+ opens, no reply, already on LinkedIn
    if opens >= 3 and replies == 0 and is_on_li:
        return {"label": "Warm on both, send personal note", "urgency": "medium"}

    # Nurture cooldown over
    if p.stage == "Nurture" and p.wake_date and p.wake_date <= now.date():
        return {"label": "Re-engage now", "urgency": "medium"}

    # Sequence completed, no response at all
    if p.instantly_status == 3 and replies == 0 and not (code and code > 0):
        if not is_on_li:
            return {"label": "Sequence done, try LinkedIn", "urgency": "low"}
        if hr_lead and not hr_lead.tag_interested and not hr_lead.tag_generic:
            return {"label": "No response on either channel", "urgency": "low"}

    return None


def next_best_actions(account_id: int) -> dict:
    """Return {prospect_id: {label, urgency}} for all leads with an action."""
    prospects = Prospect.query.filter_by(account_id=account_id).all()
    hr_map    = _build_hr_map(account_id)
    out = {}
    for p in prospects:
        k = (_norm(p.first_name or '') + _norm(p.last_name or ''), _norm(p.company_name or ''))
        action = _next_action(p, hr_map.get(k))
        if action:
            out[p.id] = action
    return out


def icp_learner(account_id: int) -> Dict:
    """
    Response rates by job title (email) and position (LinkedIn).
    Uses rates not raw counts so large title groups don't dominate.
    Only includes titles with 5+ contacts for statistical stability.
    """
    prospects = Prospect.query.filter_by(account_id=account_id).all()
    hr_leads  = HeyReachLead.query.filter_by(account_id=account_id).all()

    # Email: title → {total, responded}
    etitles: dict = defaultdict(lambda: {"total": 0, "responded": 0})
    for p in prospects:
        t = (p.job_title or '').strip()
        if len(t) < 2:
            continue
        etitles[t]["total"] += 1
        if (p.email_reply_count or 0) > 0 or (p.lt_interest_status or 0) >= 1:
            etitles[t]["responded"] += 1

    # LinkedIn: position → {total, responded}
    lpos: dict = defaultdict(lambda: {"total": 0, "responded": 0})
    for hl in hr_leads:
        pos = (hl.position or '').strip()
        if len(pos) < 2:
            continue
        lpos[pos]["total"] += 1
        if hl.tag_interested or hl.tag_generic:
            lpos[pos]["responded"] += 1

    MIN = 5  # minimum contacts to include a title

    def _rows(stats):
        rows = []
        for title, d in stats.items():
            if d["total"] < MIN:
                continue
            rows.append({"title": title[:50], "total": d["total"],
                         "responded": d["responded"],
                         "rate": _pct(d["responded"], d["total"])})
        rows.sort(key=lambda x: (-x["responded"], -x["rate"]))
        return rows[:10]

    email_rows = _rows(etitles)
    li_rows    = _rows(lpos)

    total_e_resp  = sum(1 for p  in prospects if (p.email_reply_count or 0) > 0 or (p.lt_interest_status or 0) >= 1)
    total_li_resp = sum(1 for hl in hr_leads  if hl.tag_interested or hl.tag_generic)

    return {
        "has_data":      bool(email_rows or li_rows),
        "email_titles":  email_rows,
        "li_positions":  li_rows,
        "email_total":   len(prospects),
        "email_responded": total_e_resp,
        "li_total":      len(hr_leads),
        "li_responded":  total_li_resp,
        "sample_warning": total_e_resp < 20,
    }


def lead_exhaustion(account_id: int) -> Dict:
    """
    Prospects who completed the email sequence with no reply and no interest signal.
    Splits into: try_linkedin (not on LI), ghosts (3+ opens, no reply), dead_ends (both channels, nothing).
    """
    hr_map = _build_hr_map(account_id)

    exhausted = (Prospect.query.filter_by(account_id=account_id)
                 .filter(Prospect.instantly_status == 3,
                         Prospect.email_reply_count == 0,
                         db.or_(Prospect.lt_interest_status.is_(None),
                                Prospect.lt_interest_status <= 0))
                 .filter(~Prospect.stage.in_(["Won", "Meeting", "Lost"]))
                 .all())

    try_linkedin, ghosts, dead_ends = [], [], []
    for p in exhausted:
        k   = (_norm(p.first_name or '') + _norm(p.last_name or ''), _norm(p.company_name or ''))
        hr  = hr_map.get(k)
        if (p.email_open_count or 0) >= 3:
            ghosts.append({"prospect": p, "hr_lead": hr})
        elif hr is None:
            try_linkedin.append({"prospect": p})
        else:
            dead_ends.append({"prospect": p, "hr_lead": hr})

    return {
        "total":          len(exhausted),
        "try_linkedin":   try_linkedin[:25],
        "ghosts":         ghosts[:15],
        "dead_ends":      dead_ends[:20],
        "try_li_count":   len(try_linkedin),
        "ghost_count":    len(ghosts),
        "dead_end_count": len(dead_ends),
    }


def company_heat_map(account_id: int, limit: int = 15) -> List[Dict]:
    """
    Roll up all engagement signals by company name.
    Surfaces companies where multiple people are engaging across channels.
    """
    companies: dict = defaultdict(lambda: {
        "display": "", "email_opens": 0, "email_replies": 0,
        "li_interested": 0, "li_generic": 0, "people": set(),
    })

    for p in Prospect.query.filter_by(account_id=account_id).all():
        co = _norm(p.company_name or p.company_domain or '')
        if not co:
            continue
        d = companies[co]
        if not d["display"]:
            d["display"] = p.company_name or p.company_domain or co
        d["email_opens"]   += p.email_open_count or 0
        d["email_replies"] += p.email_reply_count or 0
        d["people"].add(("email", (p.first_name or '') + ' ' + (p.last_name or '')))

    for hl in HeyReachLead.query.filter_by(account_id=account_id).all():
        co = _norm(hl.company_name or '')
        if not co:
            continue
        d = companies[co]
        if not d["display"]:
            d["display"] = hl.company_name or co
        if hl.tag_interested:
            d["li_interested"] += 1
        if hl.tag_generic:
            d["li_generic"] += 1
        d["people"].add(("li", hl.full_name))

    results = []
    for co_key, d in companies.items():
        score = (d["email_opens"] * 1 + d["email_replies"] * 5 +
                 d["li_interested"] * 8 + d["li_generic"] * 3 +
                 len(d["people"]) * 2)
        if score < 3:
            continue
        results.append({
            "company":       d["display"],
            "score":         score,
            "email_opens":   d["email_opens"],
            "email_replies": d["email_replies"],
            "li_interested": d["li_interested"],
            "li_generic":    d["li_generic"],
            "people":        len(d["people"]),
            "is_hot":        score >= 15,
            "channels":      ("both" if d["email_opens"] + d["email_replies"] > 0 and d["li_interested"] + d["li_generic"] > 0
                              else "email" if d["email_opens"] + d["email_replies"] > 0 else "linkedin"),
        })

    results.sort(key=lambda x: -x["score"])
    return results[:limit]
