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
