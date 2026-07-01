"""
Data model — the single source of truth.

Design: one Prospect spine + one Event store underneath everything.
Analytics reads events aggregated; CRM reads them per-prospect; reporting
filters by client + period. Field names mirror the REAL Instantly schema
confirmed via live introspection.
"""

from datetime import datetime
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

# ── Config constants ───────────────────────────────────────────────────────────

WARM_THRESHOLD = 3          # opens to be considered warm
ROTTING_DAYS = 7            # days of no activity before a lead is "rotting"

PIPELINE_STAGES = [
    "New", "Contacted", "Engaged", "Replied",
    "Meeting", "Won", "Lost", "Nurture",
]

LI_PIPELINE_STAGES = [
    "Contacted", "Replied", "Interested", "Meeting", "Won", "Closed",
]

DISPOSITIONS = [
    "interested", "not_now", "not_interested",
    "wrong_person", "no_reply", "unsubscribed",
]

# Channel dimension — built in now so Heyreach (LinkedIn) slots in with zero rework
CHANNEL_EMAIL = "email"
CHANNEL_LINKEDIN = "linkedin"


# ── Client (reporting boundary) ────────────────────────────────────────────────

class Client(db.Model):
    __tablename__ = "clients"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


# ── Instantly workspace account (multi-account support) ───────────────────────

class InstantlyAccount(db.Model):
    __tablename__ = "instantly_accounts"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(120), nullable=False)
    api_key = db.Column(db.String(500), nullable=False)
    workspace_name = db.Column(db.String(200), default="")
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_synced_at = db.Column(db.DateTime, nullable=True)


# ── App login user ─────────────────────────────────────────────────────────────

class AppUser(db.Model):
    __tablename__ = "app_users"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(120), nullable=False)
    email = db.Column(db.String(200), unique=True, nullable=False)
    password_hash = db.Column(db.String(200), nullable=False)
    role = db.Column(db.String(20), default="user")  # "superadmin" or "user"
    is_active = db.Column(db.Boolean, default=True)
    account_id = db.Column(db.Integer, nullable=True)   # legacy single account (kept for compat)
    account_ids = db.Column(db.Text, default="[]")       # JSON list of allowed account IDs
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_login_at = db.Column(db.DateTime, nullable=True)

    @property
    def is_superadmin(self):
        return self.role == "superadmin"

    def get_account_ids(self):
        import json as _json
        try:
            ids = _json.loads(self.account_ids or "[]")
            if ids:
                return ids
        except Exception:
            pass
        return [self.account_id] if self.account_id else []


# ── Team member / owner (Instantly) ───────────────────────────────────────────

class User(db.Model):
    __tablename__ = "users"
    id = db.Column(db.String(100), primary_key=True)   # Instantly user id or local
    name = db.Column(db.String(255), default="")
    email = db.Column(db.String(255), default="")


# ── Campaign ───────────────────────────────────────────────────────────────────

class Campaign(db.Model):
    __tablename__ = "campaigns"
    id = db.Column(db.String(100), primary_key=True)
    name = db.Column(db.String(255))
    channel = db.Column(db.String(20), default=CHANNEL_EMAIL)
    status_code = db.Column(db.Integer, default=0)
    status_label = db.Column(db.String(50), default="")
    timezone = db.Column(db.String(64), default="UTC")   # from campaign_schedule
    sending_accounts = db.Column(db.Text, default="[]")  # JSON list of mailbox emails
    client_id = db.Column(db.Integer, db.ForeignKey("clients.id"), nullable=True)

    open_tracking = db.Column(db.Boolean, default=True)
    link_tracking = db.Column(db.Boolean, default=False)

    # Aggregate analytics (from /campaigns/analytics/overview) — real field names
    emails_sent_count = db.Column(db.Integer, default=0)
    contacted_count = db.Column(db.Integer, default=0)
    new_leads_contacted_count = db.Column(db.Integer, default=0)
    open_count = db.Column(db.Integer, default=0)
    open_count_unique = db.Column(db.Integer, default=0)
    link_click_count = db.Column(db.Integer, default=0)
    link_click_count_unique = db.Column(db.Integer, default=0)
    reply_count = db.Column(db.Integer, default=0)
    reply_count_unique = db.Column(db.Integer, default=0)
    bounced_count = db.Column(db.Integer, default=0)
    unsubscribed_count = db.Column(db.Integer, default=0)
    total_opportunities = db.Column(db.Integer, default=0)
    total_opportunity_value = db.Column(db.Float, default=0.0)
    total_interested = db.Column(db.Integer, default=0)
    total_meeting_booked = db.Column(db.Integer, default=0)
    total_meeting_completed = db.Column(db.Integer, default=0)
    total_closed = db.Column(db.Integer, default=0)
    total_leads = db.Column(db.Integer, default=0)

    synced_at = db.Column(db.DateTime, default=datetime.utcnow)
    account_id = db.Column(db.Integer, nullable=True, index=True)

    # ── Derived rates (unique-based, the honest denominator) ──
    @property
    def open_rate(self):
        return _pct(self.open_count_unique, self.contacted_count)

    @property
    def reply_rate(self):
        return _pct(self.reply_count_unique, self.contacted_count)

    @property
    def click_rate(self):
        return _pct(self.link_click_count_unique, self.contacted_count)

    @property
    def bounce_rate(self):
        return _pct(self.bounced_count, self.emails_sent_count)

    @property
    def unsub_rate(self):
        return _pct(self.unsubscribed_count, self.emails_sent_count)

    @property
    def repeat_open_ratio(self):
        """total opens / unique opens — >1 means people re-open (intent signal)."""
        if not self.open_count_unique:
            return 0.0
        return round(self.open_count / self.open_count_unique, 2)

    @property
    def interested_rate(self):
        return _pct(self.total_interested, self.contacted_count)


class CampaignStep(db.Model):
    __tablename__ = "campaign_steps"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    campaign_id = db.Column(db.String(100), db.ForeignKey("campaigns.id"), index=True)
    step = db.Column(db.String(10))
    variant = db.Column(db.String(10), default="0")
    subject = db.Column(db.String(500), default="")

    sent = db.Column(db.Integer, default=0)
    opened = db.Column(db.Integer, default=0)
    unique_opened = db.Column(db.Integer, default=0)
    replies = db.Column(db.Integer, default=0)
    unique_replies = db.Column(db.Integer, default=0)
    replies_automatic = db.Column(db.Integer, default=0)
    clicks = db.Column(db.Integer, default=0)
    unique_clicks = db.Column(db.Integer, default=0)

    @property
    def open_rate(self):
        return _pct(self.unique_opened, self.sent)

    @property
    def reply_rate(self):
        return _pct(self.unique_replies, self.sent)

    @property
    def click_rate(self):
        return _pct(self.unique_clicks, self.sent)


# ── Daily metrics (for trends + heatmaps) ──────────────────────────────────────

class DailyMetric(db.Model):
    __tablename__ = "daily_metrics"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    campaign_id = db.Column(db.String(100), index=True, nullable=True)  # null = all combined
    date = db.Column(db.String(10), index=True)   # YYYY-MM-DD

    sent = db.Column(db.Integer, default=0)
    contacted = db.Column(db.Integer, default=0)
    new_leads_contacted = db.Column(db.Integer, default=0)
    opened = db.Column(db.Integer, default=0)
    unique_opened = db.Column(db.Integer, default=0)
    replies = db.Column(db.Integer, default=0)
    unique_replies = db.Column(db.Integer, default=0)
    clicks = db.Column(db.Integer, default=0)
    unique_clicks = db.Column(db.Integer, default=0)
    opportunities = db.Column(db.Integer, default=0)

    __table_args__ = (db.UniqueConstraint("campaign_id", "date", name="uix_daily"),)


# ── Prospect (the spine) ───────────────────────────────────────────────────────

class Prospect(db.Model):
    __tablename__ = "prospects"
    id = db.Column(db.String(200), primary_key=True)   # Instantly lead id
    email = db.Column(db.String(255), nullable=False, index=True)

    # Identity
    first_name = db.Column(db.String(100), default="")
    last_name = db.Column(db.String(100), default="")
    company_name = db.Column(db.String(255), default="")
    company_domain = db.Column(db.String(255), default="")
    phone = db.Column(db.String(50), default="")
    linkedin_url = db.Column(db.String(500), default="")
    job_title = db.Column(db.String(255), default="")

    # Ownership / scope
    campaign_id = db.Column(db.String(100), db.ForeignKey("campaigns.id"), index=True)
    client_id = db.Column(db.Integer, db.ForeignKey("clients.id"), nullable=True)
    owner_id = db.Column(db.String(100), nullable=True)   # assigned_to from Instantly

    # Engagement (real Instantly fields)
    email_open_count = db.Column(db.Integer, default=0)
    email_click_count = db.Column(db.Integer, default=0)
    email_reply_count = db.Column(db.Integer, default=0)
    instantly_status = db.Column(db.Integer, default=0)      # sequence status
    instantly_status_label = db.Column(db.String(50), default="")
    lt_interest_status = db.Column(db.Integer, nullable=True)   # disposition code
    interest_label = db.Column(db.String(50), nullable=True)

    # CRM lifecycle (ours)
    stage = db.Column(db.String(50), default="New")
    disposition = db.Column(db.String(50), nullable=True)
    recycle_reason = db.Column(db.String(100), nullable=True)
    wake_date = db.Column(db.Date, nullable=True)
    reengagement_note = db.Column(db.Text, default="")
    lost_reason = db.Column(db.String(100), nullable=True)
    do_not_contact = db.Column(db.Boolean, default=False)
    notes = db.Column(db.Text, default="")

    # Intelligence
    warm_score = db.Column(db.Float, default=0.0)
    last_activity_at = db.Column(db.DateTime, nullable=True)
    stage_changed_at = db.Column(db.DateTime, nullable=True)

    timestamp_last_open = db.Column(db.DateTime, nullable=True)
    timestamp_last_reply = db.Column(db.DateTime, nullable=True)
    timestamp_last_contact = db.Column(db.DateTime, nullable=True)
    synced_at = db.Column(db.DateTime, default=datetime.utcnow)
    account_id = db.Column(db.Integer, nullable=True, index=True)

    @property
    def full_name(self):
        import re as _re
        _email = _re.compile(r'\S+@\S+\.\S+')
        fn = _email.sub('', self.first_name or '').strip()
        ln = _email.sub('', self.last_name or '').strip()
        n = f"{fn} {ln}".strip()
        return n or (self.email.split("@")[0] if self.email else "Unknown")

    @property
    def is_warm(self):
        return self.email_open_count >= WARM_THRESHOLD

    @property
    def has_replied(self):
        return self.email_reply_count > 0


# ── Event store (the atom) ─────────────────────────────────────────────────────

class Event(db.Model):
    __tablename__ = "events"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    prospect_id = db.Column(db.String(200), index=True, nullable=True)
    prospect_email = db.Column(db.String(255), index=True)
    campaign_id = db.Column(db.String(100), index=True, nullable=True)
    channel = db.Column(db.String(20), default=CHANNEL_EMAIL)

    # type: email_sent/opened/clicked/replied/bounced/unsubscribed
    #       interested/not_interested/meeting_booked/...
    #       (CRM) stage_changed/disposition_set/parked/re_engaged/note_added
    type = db.Column(db.String(50), index=True)
    occurred_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    source = db.Column(db.String(30), default="sync")   # sync | webhook | crm
    meta = db.Column(db.Text, default="")               # JSON blob
    account_id = db.Column(db.Integer, nullable=True, index=True)

    __table_args__ = (
        db.UniqueConstraint("prospect_email", "type", "occurred_at",
                            "campaign_id", name="uix_event"),
    )


# ── Sending account (deliverability) ───────────────────────────────────────────

class SendingAccount(db.Model):
    __tablename__ = "sending_accounts"
    email = db.Column(db.String(255), primary_key=True)
    status_code = db.Column(db.Integer, default=0)
    warmup_status = db.Column(db.Integer, default=0)
    warmup_score = db.Column(db.Integer, default=0)       # stat_warmup_score
    provider_code = db.Column(db.Integer, default=0)
    setup_pending = db.Column(db.Boolean, default=False)
    error_message = db.Column(db.String(500), default="")  # from status_message (real account error)

    # From warmup-analytics
    health_score = db.Column(db.Integer, default=0)
    wa_sent = db.Column(db.Integer, default=0)
    wa_landed_inbox = db.Column(db.Integer, default=0)
    wa_landed_spam = db.Column(db.Integer, default=0)
    wa_received = db.Column(db.Integer, default=0)

    account_id = db.Column(db.Integer, nullable=True, index=True)
    synced_at = db.Column(db.DateTime, default=datetime.utcnow)

    @property
    def has_placement_data(self):
        return self.wa_sent > 0

    @property
    def inbox_rate(self):
        total = self.wa_landed_inbox + self.wa_landed_spam
        return _pct(self.wa_landed_inbox, total) if total else 0.0

    @property
    def health_state(self):
        """Accurate state from Instantly's real account status enum
        (1=Active, 2=Paused, -1=Connection err, -2=Soft bounce, -3=Sending err).
        A 0 warmup score with no error is NOT an error; a paused account is NOT an error."""
        if self.status_code < 0:
            return "error"                       # real connection / bounce / sending error
        if self.status_code == 2:
            return "paused"                      # intentionally paused — not an issue
        # status 1 (Active)
        if self.has_placement_data and self.inbox_rate < 80:
            return "spam"                        # warming but landing in spam
        if self.warmup_score >= 75 or (self.has_placement_data and self.inbox_rate >= 90):
            return "healthy"                     # good warmup score or strong inbox placement
        return "ok"                              # active, no error, just not warming / no data

    @property
    def state_label(self):
        s = self.health_state
        if s == "error":
            return {-1: "Connection error", -2: "Soft bounce error",
                    -3: "Sending error"}.get(self.status_code, self.error_message or "Error")
        return {"spam": "Spam risk", "healthy": "Healthy", "ok": "Active", "paused": "Paused"}[s]

    @property
    def state_color(self):
        return {"error": "red", "spam": "yellow", "healthy": "green",
                "ok": "gray", "paused": "gray"}[self.health_state]

    @property
    def is_issue(self):
        """Genuinely needs attention: a real account error or spam placement.
        Paused / warmup-off mailboxes are NOT issues."""
        return self.health_state in ("error", "spam")


# ── Email messages (inbox: full body, thread view) ────────────────────────────

class EmailMessage(db.Model):
    __tablename__ = "email_messages"
    id = db.Column(db.String(100), primary_key=True)        # Instantly email UUID
    thread_id = db.Column(db.String(200), index=True)
    lead_id = db.Column(db.String(200), index=True, nullable=True)
    prospect_email = db.Column(db.String(255), index=True, nullable=True)
    campaign_id = db.Column(db.String(100), index=True, nullable=True)
    eaccount = db.Column(db.String(255), nullable=True)     # our sending mailbox
    from_address = db.Column(db.String(255), nullable=True)
    to_address = db.Column(db.String(500), nullable=True)
    subject = db.Column(db.String(500), default="")
    body_html = db.Column(db.Text, default="")
    body_text = db.Column(db.Text, default="")              # html-stripped preview
    ue_type = db.Column(db.Integer, default=1)              # 1=sent, 2=received
    is_unread = db.Column(db.Boolean, default=False)
    is_focused = db.Column(db.Boolean, default=False)
    step = db.Column(db.String(10), nullable=True)
    timestamp_email = db.Column(db.DateTime, nullable=True)
    timestamp_created = db.Column(db.DateTime, nullable=True)
    synced_at = db.Column(db.DateTime, default=datetime.utcnow)

    @property
    def is_inbound(self):
        return self.ue_type == 2

    @property
    def preview(self):
        text = self.body_text or ""
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        return " ".join(lines)[:200] if lines else ""


# ── Client dashboard token ─────────────────────────────────────────────────────

class ClientToken(db.Model):
    __tablename__ = "client_tokens"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    token = db.Column(db.String(64), unique=True, nullable=False, index=True)
    account_id = db.Column(db.Integer, nullable=False, index=True)
    label = db.Column(db.String(200), default="")
    expires_at = db.Column(db.DateTime, nullable=True)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    @property
    def is_valid(self):
        if not self.is_active:
            return False
        if self.expires_at and self.expires_at < datetime.utcnow():
            return False
        return True

    @property
    def account(self):
        from models import db as _db
        from sqlalchemy.orm import object_session
        sess = object_session(self) or _db.session
        return sess.get(InstantlyAccount, self.account_id)


# ── Client activity log ────────────────────────────────────────────────────────

class ClientActivity(db.Model):
    __tablename__ = "client_activities"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    token_id = db.Column(db.Integer, nullable=False, index=True)
    account_id = db.Column(db.Integer, nullable=False, index=True)
    prospect_id = db.Column(db.String(200), nullable=True)
    prospect_email = db.Column(db.String(255), nullable=True)
    action = db.Column(db.String(50))   # outcome_won | outcome_lost | outcome_reschedule | note_added
    value = db.Column(db.String(200), default="")
    note = db.Column(db.Text, default="")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


# ── HeyReach (LinkedIn outreach) ──────────────────────────────────────────────

class HeyReachAccount(db.Model):
    __tablename__ = "heyreach_accounts"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(200), default="HeyReach")
    api_key = db.Column(db.String(500), nullable=False)
    account_id = db.Column(db.Integer, nullable=False, index=True)   # links to InstantlyAccount.id scope
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    synced_at = db.Column(db.DateTime, nullable=True)


class HeyReachCampaign(db.Model):
    __tablename__ = "heyreach_campaigns"
    id = db.Column(db.Integer, primary_key=True)          # HeyReach campaign id
    name = db.Column(db.String(500), default="")
    status = db.Column(db.String(50), default="")         # ACTIVE|FINISHED|FAILED|PAUSED
    list_id = db.Column(db.Integer, nullable=True, index=True)
    list_name = db.Column(db.String(300), default="")
    total_leads = db.Column(db.Integer, default=0)
    leads_in_progress = db.Column(db.Integer, default=0)
    leads_finished = db.Column(db.Integer, default=0)
    leads_failed = db.Column(db.Integer, default=0)
    started_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, nullable=True)
    heyreach_account_id = db.Column(db.Integer, db.ForeignKey("heyreach_accounts.id"), index=True)
    account_id = db.Column(db.Integer, nullable=False, index=True)
    synced_at = db.Column(db.DateTime, default=datetime.utcnow)

    @property
    def status_label(self):
        return {"ACTIVE": "Active", "FINISHED": "Finished",
                "FAILED": "Failed", "PAUSED": "Paused"}.get(self.status, self.status)

    @property
    def interested_count(self):
        return HeyReachLead.query.filter(
            HeyReachLead.campaign_id == self.id,
            HeyReachLead.tag_interested == True
        ).count()

    @property
    def not_interested_count(self):
        return HeyReachLead.query.filter(
            HeyReachLead.campaign_id == self.id,
            HeyReachLead.tag_not_interested == True
        ).count()


class HeyReachLead(db.Model):
    __tablename__ = "heyreach_leads"
    id = db.Column(db.String(100), primary_key=True)      # HeyReach lead id
    linkedin_id = db.Column(db.String(100), nullable=True)
    linkedin_url = db.Column(db.String(500), default="")
    first_name = db.Column(db.String(200), default="")
    last_name = db.Column(db.String(200), default="")
    headline = db.Column(db.String(500), default="")
    position = db.Column(db.String(300), default="")
    company_name = db.Column(db.String(300), default="")
    location = db.Column(db.String(300), default="")
    email = db.Column(db.String(255), nullable=True)       # enriched when available

    # Outcome tags from HeyReach
    tag_interested = db.Column(db.Boolean, default=False)
    tag_not_interested = db.Column(db.Boolean, default=False)
    tag_generic = db.Column(db.Boolean, default=False)
    raw_tags = db.Column(db.Text, default="")              # JSON list of tag names

    # CRM pipeline stage (managed locally, independent of HeyReach tags)
    li_stage = db.Column(db.String(50), nullable=True)
    li_stage_changed_at = db.Column(db.DateTime, nullable=True)

    # Campaign link (a lead can appear in multiple campaigns via list)
    campaign_id = db.Column(db.Integer, db.ForeignKey("heyreach_campaigns.id"), nullable=True, index=True)
    list_id = db.Column(db.Integer, nullable=True, index=True)
    heyreach_account_id = db.Column(db.Integer, db.ForeignKey("heyreach_accounts.id"), index=True)
    account_id = db.Column(db.Integer, nullable=False, index=True)
    synced_at = db.Column(db.DateTime, default=datetime.utcnow)

    @property
    def full_name(self):
        return f"{self.first_name} {self.last_name}".strip() or "Unknown"

    @property
    def outcome_tag(self):
        if self.tag_interested:
            return "Interested"
        if self.tag_not_interested:
            return "Not Interested"
        if self.tag_generic:
            return "Generic"
        return None

    @property
    def name_company_key(self):
        """Normalised key for cross-channel matching."""
        import re as _re
        def _norm(s):
            return _re.sub(r'[^a-z0-9]', '', (s or '').lower())
        return (_norm(self.first_name) + _norm(self.last_name), _norm(self.company_name))


# ── Meta (key/value: workspace name, last sync, etc.) ──────────────────────────

class Meta(db.Model):
    __tablename__ = "meta"
    key = db.Column(db.String(50), primary_key=True)
    value = db.Column(db.String(500))

    @staticmethod
    def get(key, default=None):
        row = db.session.get(Meta, key)
        return row.value if row else default

    @staticmethod
    def set(key, value):
        row = db.session.get(Meta, key) or Meta(key=key)
        row.value = str(value)
        db.session.add(row)


# ── helpers ────────────────────────────────────────────────────────────────────

def _pct(num, denom):
    return round(num / denom * 100, 1) if denom else 0.0
