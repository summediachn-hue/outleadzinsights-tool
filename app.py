import csv
import hashlib
import hmac
import html
import io
import json
import logging
import os
import re
import threading
import uuid
from datetime import datetime, date, timedelta
from functools import wraps

from dotenv import load_dotenv
from flask import Flask, flash, g, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import generate_password_hash as _gen_pw, check_password_hash

def generate_password_hash(pw):
    return _gen_pw(pw, method="pbkdf2:sha256")

load_dotenv()

from models import (
    db, PIPELINE_STAGES, LI_PIPELINE_STAGES, DISPOSITIONS, WARM_THRESHOLD, ROTTING_DAYS,
    Campaign, CampaignStep, Prospect, Event, SendingAccount, Meta, EmailMessage,
    InstantlyAccount, AppUser, ClientToken, ClientActivity,
    HeyReachAccount, HeyReachCampaign, HeyReachLead,
    CalendlyAccount,
)
from instantly_client import InstantlyClient, InstantlyError
from sync import run_sync
from heyreach_client import HeyReachClient, HeyReachError
from heyreach_sync import run_heyreach_sync
from calendly_client import CalendlyClient, CalendlyError
from calendly_sync import run_calendly_sync
import analytics as an

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

BASEDIR = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "outreach-analytics-dev-key")
app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{os.path.join(BASEDIR, 'instance', 'outreach.db')}"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "connect_args": {"timeout": 30},  # wait up to 30s for a lock instead of failing immediately
}
os.makedirs(os.path.join(BASEDIR, "instance"), exist_ok=True)

db.init_app(app)

# Apply WAL mode on every new SQLite connection — prevents "database is locked"
# when the sync background thread and web requests write at the same time.
from sqlalchemy import event as _sa_event
from sqlalchemy.engine import Engine as _Engine
import sqlite3 as _sqlite3

@_sa_event.listens_for(_Engine, "connect")
def _set_sqlite_pragmas(dbapi_conn, _rec):
    if isinstance(dbapi_conn, _sqlite3.Connection):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA busy_timeout=30000")
        cur.close()

with app.app_context():
    db.create_all()

    # One-time fix: rebuild calendly_accounts if it still has the legacy
    # webhook_secret NOT NULL column (created before the PAT-based approach).
    try:
        _db_path = os.path.join(BASEDIR, "instance", "outreach.db")
        _raw = _sqlite3.connect(_db_path)
        _row = _raw.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='calendly_accounts'"
        ).fetchone()
        if _row and "webhook_secret" in _row[0]:
            _raw.execute(
                "CREATE TABLE IF NOT EXISTS _cal_fix ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                "account_id INTEGER NOT NULL,"
                "api_token VARCHAR(500) NOT NULL DEFAULT '',"
                "user_uri VARCHAR(500) DEFAULT '',"
                "organization_uri VARCHAR(500) DEFAULT '',"
                "is_active BOOLEAN DEFAULT 1,"
                "created_at DATETIME,"
                "last_synced_at DATETIME,"
                "last_booking_at DATETIME,"
                "booking_count INTEGER DEFAULT 0)"
            )
            _raw.execute(
                "INSERT OR IGNORE INTO _cal_fix "
                "(id, account_id, api_token, user_uri, organization_uri, is_active,"
                " created_at, last_synced_at, last_booking_at, booking_count) "
                "SELECT id, account_id, COALESCE(api_token,''), COALESCE(user_uri,''),"
                "       COALESCE(organization_uri,''), COALESCE(is_active,1), created_at,"
                "       last_synced_at, last_booking_at, COALESCE(booking_count,0) "
                "FROM calendly_accounts"
            )
            _raw.execute("DROP TABLE calendly_accounts")
            _raw.execute("ALTER TABLE _cal_fix RENAME TO calendly_accounts")
            _raw.commit()
        _raw.close()
    except Exception:
        pass

    for stmt in [
        "ALTER TABLE prospects ADD COLUMN stage_changed_at DATETIME",
        "ALTER TABLE campaigns ADD COLUMN account_id INTEGER",
        "ALTER TABLE prospects ADD COLUMN account_id INTEGER",
        "ALTER TABLE app_users ADD COLUMN account_ids TEXT DEFAULT '[]'",
        "ALTER TABLE sending_accounts ADD COLUMN account_id INTEGER",
        "UPDATE sending_accounts SET error_message = '' WHERE status_code >= 0",
        ("CREATE TABLE IF NOT EXISTS client_tokens ("
         "id INTEGER PRIMARY KEY AUTOINCREMENT,"
         "token VARCHAR(64) UNIQUE NOT NULL,"
         "account_id INTEGER NOT NULL,"
         "label VARCHAR(200) DEFAULT '',"
         "expires_at DATETIME,"
         "is_active BOOLEAN DEFAULT 1,"
         "created_at DATETIME)"),
        ("CREATE TABLE IF NOT EXISTS client_activities ("
         "id INTEGER PRIMARY KEY AUTOINCREMENT,"
         "token_id INTEGER NOT NULL,"
         "account_id INTEGER NOT NULL,"
         "prospect_id VARCHAR(200),"
         "prospect_email VARCHAR(255),"
         "action VARCHAR(50),"
         "value VARCHAR(200) DEFAULT '',"
         "note TEXT DEFAULT '',"
         "created_at DATETIME)"),
        # Heal prospects whose account_id doesn't match their campaign's account_id
        ("UPDATE prospects SET account_id = ("
         "SELECT campaigns.account_id FROM campaigns "
         "WHERE campaigns.id = prospects.campaign_id AND campaigns.account_id IS NOT NULL"
         ") WHERE campaign_id IS NOT NULL AND ("
         "prospects.account_id IS NULL OR prospects.account_id != ("
         "SELECT campaigns.account_id FROM campaigns WHERE campaigns.id = prospects.campaign_id))"),
        # Events: add account_id column (idempotent via try/except)
        "ALTER TABLE events ADD COLUMN account_id INTEGER",
        # Heal events.account_id from their campaign
        ("UPDATE events SET account_id = ("
         "SELECT campaigns.account_id FROM campaigns "
         "WHERE campaigns.id = events.campaign_id"
         ") WHERE campaign_id IS NOT NULL AND account_id IS NULL"),
        # Indexes for multi-tenant query performance
        "CREATE INDEX IF NOT EXISTS ix_campaigns_account_id ON campaigns(account_id)",
        "CREATE INDEX IF NOT EXISTS ix_prospects_account_id ON prospects(account_id)",
        "CREATE INDEX IF NOT EXISTS ix_events_account_id ON events(account_id)",
        "CREATE INDEX IF NOT EXISTS ix_sending_accounts_account_id ON sending_accounts(account_id)",
        # HeyReach tables
        ("CREATE TABLE IF NOT EXISTS heyreach_accounts ("
         "id INTEGER PRIMARY KEY AUTOINCREMENT,"
         "name VARCHAR(200) DEFAULT 'HeyReach',"
         "api_key VARCHAR(500) NOT NULL,"
         "account_id INTEGER NOT NULL,"
         "is_active BOOLEAN DEFAULT 1,"
         "created_at DATETIME,"
         "synced_at DATETIME)"),
        ("CREATE TABLE IF NOT EXISTS heyreach_campaigns ("
         "id INTEGER PRIMARY KEY,"
         "name VARCHAR(500) DEFAULT '',"
         "status VARCHAR(50) DEFAULT '',"
         "list_id INTEGER,"
         "list_name VARCHAR(300) DEFAULT '',"
         "total_leads INTEGER DEFAULT 0,"
         "leads_in_progress INTEGER DEFAULT 0,"
         "leads_finished INTEGER DEFAULT 0,"
         "leads_failed INTEGER DEFAULT 0,"
         "started_at DATETIME,"
         "created_at DATETIME,"
         "heyreach_account_id INTEGER,"
         "account_id INTEGER NOT NULL,"
         "synced_at DATETIME)"),
        ("CREATE TABLE IF NOT EXISTS heyreach_leads ("
         "id VARCHAR(100) PRIMARY KEY,"
         "linkedin_id VARCHAR(100),"
         "linkedin_url VARCHAR(500) DEFAULT '',"
         "first_name VARCHAR(200) DEFAULT '',"
         "last_name VARCHAR(200) DEFAULT '',"
         "headline VARCHAR(500) DEFAULT '',"
         "position VARCHAR(300) DEFAULT '',"
         "company_name VARCHAR(300) DEFAULT '',"
         "location VARCHAR(300) DEFAULT '',"
         "email VARCHAR(255),"
         "tag_interested BOOLEAN DEFAULT 0,"
         "tag_not_interested BOOLEAN DEFAULT 0,"
         "tag_generic BOOLEAN DEFAULT 0,"
         "raw_tags TEXT DEFAULT '',"
         "campaign_id INTEGER,"
         "list_id INTEGER,"
         "heyreach_account_id INTEGER,"
         "account_id INTEGER NOT NULL,"
         "synced_at DATETIME)"),
        "CREATE INDEX IF NOT EXISTS ix_heyreach_leads_account_id ON heyreach_leads(account_id)",
        "CREATE INDEX IF NOT EXISTS ix_heyreach_leads_campaign_id ON heyreach_leads(campaign_id)",
        "CREATE INDEX IF NOT EXISTS ix_heyreach_campaigns_account_id ON heyreach_campaigns(account_id)",
        # HeyReach leads CRM stage (added later)
        "ALTER TABLE heyreach_leads ADD COLUMN li_stage VARCHAR(50)",
        "ALTER TABLE heyreach_leads ADD COLUMN li_stage_changed_at DATETIME",
        # Calendly inbound
        ("CREATE TABLE IF NOT EXISTS calendly_accounts ("
         "id INTEGER PRIMARY KEY AUTOINCREMENT,"
         "account_id INTEGER NOT NULL,"
         "api_token VARCHAR(500) NOT NULL DEFAULT '',"
         "user_uri VARCHAR(500) DEFAULT '',"
         "organization_uri VARCHAR(500) DEFAULT '',"
         "is_active BOOLEAN DEFAULT 1,"
         "created_at DATETIME,"
         "last_synced_at DATETIME,"
         "last_booking_at DATETIME,"
         "booking_count INTEGER DEFAULT 0)"),
        "CREATE INDEX IF NOT EXISTS ix_calendly_accounts_account_id ON calendly_accounts(account_id)",
        # Migrate existing rows that used webhook_secret column
        "ALTER TABLE calendly_accounts ADD COLUMN api_token VARCHAR(500) DEFAULT ''",
        "ALTER TABLE calendly_accounts ADD COLUMN user_uri VARCHAR(500) DEFAULT ''",
        "ALTER TABLE calendly_accounts ADD COLUMN organization_uri VARCHAR(500) DEFAULT ''",
        "ALTER TABLE calendly_accounts ADD COLUMN last_synced_at DATETIME",
        "ALTER TABLE prospects ADD COLUMN source VARCHAR(30) DEFAULT 'outreach'",
        "ALTER TABLE prospects ADD COLUMN calendly_event_type VARCHAR(200) DEFAULT ''",
        "ALTER TABLE prospects ADD COLUMN calendly_scheduled_at DATETIME",
    ]:
        try:
            db.session.execute(db.text(stmt))
            db.session.commit()
        except Exception:
            db.session.rollback()

    # Migrate legacy account_id → account_ids for existing users
    for u in AppUser.query.all():
        if (not u.account_ids or u.account_ids == "[]") and u.account_id:
            u.account_ids = json.dumps([u.account_id])
    db.session.commit()

    # Seed default InstantlyAccount from .env key if none exist
    if InstantlyAccount.query.count() == 0:
        env_key = os.getenv("INSTANTLY_API_KEY", "").strip()
        if env_key:
            ws = Meta.get("workspace_name", "Default Account")
            acct = InstantlyAccount(name="Default Account", api_key=env_key, workspace_name=ws)
            db.session.add(acct)
            db.session.commit()
            db.session.execute(db.text("UPDATE campaigns SET account_id=1 WHERE account_id IS NULL"))
            db.session.execute(db.text("UPDATE prospects SET account_id=1 WHERE account_id IS NULL"))
            db.session.commit()

    # Seed default super admin on first run — credentials from env or fallback defaults
    if AppUser.query.count() == 0:
        _seed_email = os.getenv("ADMIN_EMAIL", "summediachn@gmail.com").strip()
        _seed_pw = os.getenv("ADMIN_PASSWORD", "Outleadz2025!").strip()
        admin = AppUser(
            name="Super Admin",
            email=_seed_email,
            password_hash=generate_password_hash(_seed_pw),
            role="superadmin",
        )
        db.session.add(admin)
        db.session.commit()
        log.info(f"Seeded super admin: {_seed_email}")
        log.info("Seeded super admin: summediachn@gmail.com / Outleadz2025!")


def _client(account_id=None):
    aid = account_id or getattr(g, 'account_id', None)
    if aid:
        acct = db.session.get(InstantlyAccount, aid)
        if acct and acct.is_active and acct.api_key:
            return InstantlyClient(acct.api_key.strip())
    key = os.getenv("INSTANTLY_API_KEY", "").strip()
    return InstantlyClient(key) if key else None


# ── Template globals ───────────────────────────────────────────────────────────

@app.context_processor
def inject_nav_context():
    """Expose channel flags to every template — drives nav visibility and page guards."""
    return {
        "hr_nav": getattr(g, "hr_account", None),
        "has_instantly": getattr(g, "has_instantly", True),
        "has_heyreach": getattr(g, "has_heyreach", False),
    }


def _email_channel_required():
    """Return a redirect if the active client has no Instantly key, else None."""
    if not g.account_id:
        return None  # superadmin — no restriction
    if g.has_instantly:
        return None  # all good
    if g.has_heyreach:
        return redirect(url_for("linkedin_leads"))
    return redirect(url_for("dashboard"))


# ── Auth ───────────────────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login_page'))
        return f(*args, **kwargs)
    return decorated


def superadmin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login_page'))
        if session.get('role') != 'superadmin':
            flash('Super admin access required.', 'error')
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated


@app.before_request
def load_user():
    g.user = None
    g.account_id = None
    g.user_accounts = []
    g.has_instantly = True   # safe default — superadmin / unauthenticated sees all
    g.has_heyreach = False
    g.hr_account = None
    uid = session.get('user_id')
    if uid:
        u = AppUser.query.get(uid)
        if u and u.is_active:
            g.user = u
            if u.role == 'superadmin':
                g.user_accounts = InstantlyAccount.query.filter_by(is_active=True).all()
                g.account_id = session.get('active_account_id')
                if not g.account_id and g.user_accounts:
                    g.account_id = g.user_accounts[0].id
                    session['active_account_id'] = g.account_id
            else:
                allowed = u.get_account_ids()
                g.user_accounts = (InstantlyAccount.query
                    .filter(InstantlyAccount.id.in_(allowed),
                            InstantlyAccount.is_active == True).all()
                    if allowed else [])
                active = session.get('active_account_id')
                if active and active in allowed:
                    g.account_id = active
                elif g.user_accounts:
                    g.account_id = g.user_accounts[0].id
                    session['active_account_id'] = g.account_id
            # Compute channel flags for the selected account
            if g.account_id:
                _acct = db.session.get(InstantlyAccount, g.account_id)
                g.has_instantly = bool(_acct and _acct.api_key)
                g.hr_account = HeyReachAccount.query.filter_by(
                    account_id=g.account_id, is_active=True
                ).first()
                g.has_heyreach = g.hr_account is not None
        else:
            session.clear()


def _pq():
    """Prospect query scoped to current account."""
    q = Prospect.query
    if g.account_id:
        q = q.filter(Prospect.account_id == g.account_id)
    return q


def _cq():
    """Campaign query scoped to current account."""
    q = Campaign.query
    if g.account_id:
        q = q.filter(Campaign.account_id == g.account_id)
    return q


def _parse_range(args):
    """Parse ?start/?end (custom) or ?days into (start_date, end_date, active_token)."""
    from datetime import date as _date
    start, end = args.get("start"), args.get("end")
    if start or end:
        try:
            s = _date.fromisoformat(start) if start else None
            e = _date.fromisoformat(end) if end else _date.today()
            return s, e, "custom"
        except ValueError:
            pass
    days = args.get("days", "30")
    if days == "all":
        return None, None, "all"
    try:
        n = max(1, int(days))
    except (TypeError, ValueError):
        n, days = 30, "30"
    return _date.today() - timedelta(days=n), _date.today(), days


@app.template_filter('time_ago')
def time_ago_filter(dt):
    if not dt:
        return ''
    delta = datetime.utcnow() - dt
    days = delta.days
    hours = delta.seconds // 3600
    if days >= 2:
        return f'{days}d ago'
    if days == 1:
        return 'yesterday'
    if hours >= 1:
        return f'{hours}h ago'
    mins = delta.seconds // 60
    return f'{mins}m ago' if mins >= 1 else 'just now'


@app.context_processor
def inject_globals():
    last = Meta.get("last_sync")
    last_dt = None
    if last:
        try:
            last_dt = datetime.fromisoformat(last)
        except ValueError:
            pass
    ws_name = "Outleadz"
    cur_user = getattr(g, 'user', None)
    act_id = getattr(g, 'account_id', None)
    user_accounts = getattr(g, 'user_accounts', [])
    if act_id:
        acct = db.session.get(InstantlyAccount, act_id)
        if acct:
            ws_name = acct.workspace_name or acct.name
            if acct.last_synced_at:
                last_dt = acct.last_synced_at
    else:
        ws_name = Meta.get("workspace_name", "Outleadz")
    return {
        "warm_threshold": WARM_THRESHOLD,
        "has_key": True,
        "workspace_name": ws_name,
        "last_sync": last_dt,
        "current_user": cur_user,
        "user_accounts": user_accounts,   # accounts this user can access
        "active_account_id": act_id,
    }


# ── Dashboard (Engagement + Funnel) ────────────────────────────────────────────

TIMEFRAMES = [("7", "Last 7 days"), ("30", "Last 30 days"),
              ("90", "Last 90 days"), ("all", "All time")]


@app.route("/")
@login_required
def dashboard():
    start, end, active = _parse_range(request.args)
    aid = g.account_id
    hr_account = g.hr_account
    life = an.lifetime_stats(account_id=aid)

    wk = an.weekly_kpis(aid) if aid else {}
    wins = an.recent_wins(aid, limit=8) if aid else []

    forecast = None
    if aid and life.get("interested", 0) > 0:
        hist_rate = (life["meetings"] / life["replies"]) if (life.get("replies") and life.get("meetings")) else 0.35
        forecast = {
            "interested": life["interested"],
            "rate_pct": round(hist_rate * 100),
            "min": max(1, int(life["interested"] * hist_rate * 0.7)),
            "max": int(life["interested"] * hist_rate * 1.3) + 1,
        }

    return render_template("dashboard.html",
        has_instantly=g.has_instantly,
        metrics=an.dashboard_metrics(start, end, account_id=aid),
        life=life,
        funnel=an.funnel(start, end, account_id=aid),
        timing=an.engagement_timing(None, start, end, account_id=aid),
        account=an.account_recommendations(account_id=aid),
        warm=an.get_warm_leads(limit=8, account_id=aid),
        hidden=an.hidden_intent_leads(limit=8, account_id=aid),
        timeframes=TIMEFRAMES,
        active_tf=active,
        start_str=request.args.get("start", ""),
        end_str=request.args.get("end", ""),
        hr_account=hr_account,
        hr_kpis=an.heyreach_kpis(aid) if hr_account and aid else None,
        cross_leads=an.cross_channel_leads(aid, limit=20) if hr_account and aid else [],
        uni_funnel=an.unified_funnel(aid) if aid else [],
        gaps=an.gap_lists(aid) if aid else {"email_warm_not_li": [], "li_interested_not_email": []},
        lift=an.channel_lift(aid) if (g.has_instantly and g.has_heyreach and aid) else None,
        wk=wk, wins=wins, forecast=forecast,
    )


# ── Sync ───────────────────────────────────────────────────────────────────────

@app.route("/sync", methods=["POST"])
@login_required
def sync():
    client = _client()
    if not client:
        flash("No Instantly API key configured.", "error")
        return redirect(request.referrer or url_for("dashboard"))
    try:
        s = run_sync(client, account_id=g.account_id)
        if g.account_id:
            acct = db.session.get(InstantlyAccount, g.account_id)
            if acct:
                acct.last_synced_at = datetime.utcnow()
                db.session.commit()
    except Exception as e:
        log.exception("sync failed")
        flash(f"Sync failed: {e}", "error")
        return redirect(request.referrer or url_for("dashboard"))
    msg = (f"Synced {s['campaigns']} campaigns · {s['leads']} leads · "
           f"{s['events']} events · {s['accounts']} mailboxes · "
           f"{s.get('emails', 0)} emails.")
    if s["errors"]:
        msg += f" ({len(s['errors'])} warnings)"
    flash(msg, "success")
    return redirect(request.referrer or url_for("dashboard"))


# ── Removed tabs → redirect (content now lives in Dashboard / Campaigns) ───────

@app.route("/recommendations")
@login_required
def recommendations():
    return redirect(url_for("dashboard"))


@app.route("/trends")
@login_required
def trends():
    return redirect(url_for("dashboard"))


@app.route("/scripts")
@login_required
def scripts():
    return redirect(url_for("campaigns"))


# ── Campaigns ──────────────────────────────────────────────────────────────────

@app.route("/campaigns")
@login_required
def campaigns():
    channel = request.args.get("ch", "all")  # all | email | linkedin
    aid = g.account_id
    has_instantly = g.has_instantly
    hr_account = g.hr_account
    email_data = []
    if has_instantly and channel in ("all", "email"):
        all_c = _cq().order_by(Campaign.emails_sent_count.desc()).all()
        for c in all_c:
            warm = (_pq().filter_by(campaign_id=c.id)
                    .filter(Prospect.email_open_count >= WARM_THRESHOLD).count())
            email_data.append({"c": c, "diag": an.diagnose_campaign(c), "warm": warm})
    hr_campaigns = []
    if channel in ("all", "linkedin") and aid:
        hr_campaigns = an.heyreach_campaigns(aid)
    return render_template("campaigns.html",
        data=email_data,
        hr_campaigns=hr_campaigns,
        hr_account=hr_account,
        has_instantly=has_instantly,
        active_channel=channel,
    )


@app.route("/linkedin-leads")
@login_required
def linkedin_leads():
    aid = g.account_id
    hr_account = HeyReachAccount.query.filter_by(account_id=aid, is_active=True).first() if aid else None
    if not hr_account:
        flash("No HeyReach account connected. Add one in Admin → Clients.", "error")
        return redirect(url_for("campaigns"))

    campaign_id_raw = request.args.get("campaign", "")
    campaign_id = int(campaign_id_raw) if campaign_id_raw.isdigit() else None
    tag = request.args.get("tag", "all")
    search = request.args.get("q", "").strip()

    tag_filter = None if tag == "all" else tag
    leads = an.linkedin_leads_filtered(aid, campaign_id=campaign_id, tag=tag_filter, search=search or None)
    campaigns = an.heyreach_campaigns(aid)
    campaign_map = {c.id: c.name for c in campaigns}
    insights = an.linkedin_audience_insights(aid, campaign_id=campaign_id)
    matched = an.linkedin_cross_match(aid, leads) if aid else {}

    # Stats scoped to campaign filter if active
    base_q = HeyReachLead.query.filter_by(account_id=aid)
    if campaign_id:
        base_q = base_q.filter(HeyReachLead.campaign_id == campaign_id)
    total = base_q.count()
    interested = base_q.filter(HeyReachLead.tag_interested == True).count()
    not_interested = base_q.filter(HeyReachLead.tag_not_interested == True).count()
    generic = base_q.filter(HeyReachLead.tag_generic == True).count()

    return render_template("linkedin_leads.html",
        hr_account=hr_account,
        leads=leads,
        campaigns=campaigns,
        campaign_map=campaign_map,
        matched=matched,
        insights=insights,
        active_campaign=campaign_id,
        active_tag=tag,
        search=search,
        stats={
            "total": total,
            "interested": interested,
            "not_interested": not_interested,
            "generic": generic,
            "untagged": max(0, total - interested - not_interested - generic),
        },
    )


@app.route("/campaign/<cid>")
@login_required
def campaign_detail(cid):
    c = db.get_or_404(Campaign, cid)
    if not request.args.get("days") and not request.args.get("start"):
        start, end, active = None, None, "all"
    else:
        start, end, active = _parse_range(request.args)
    leads = (_pq().filter_by(campaign_id=cid)
             .order_by(Prospect.warm_score.desc()).all())
    return render_template("campaign_detail.html",
        c=c, recs=an.campaign_recommendations(c),
        steps=an.sequence_funnel(cid),
        leads=leads[:60],
        warm=[l for l in leads if l.is_warm],
        replied=[l for l in leads if l.has_replied],
        daily=an.daily_series(cid, start=start, end=end),
        ab=an.ab_variants(cid),
        timing=an.engagement_timing(cid, start, end),
        timeframes=TIMEFRAMES,
        active_tf=active,
        start_str=request.args.get("start", ""),
        end_str=request.args.get("end", ""),
    )


# ── Deliverability ─────────────────────────────────────────────────────────────

@app.route("/intelligence")
@login_required
def intelligence():
    aid = g.account_id
    start, end, active = _parse_range(request.args)
    return render_template("intelligence.html",
        send_time=an.best_send_time(aid, start=start, end=end) if (g.has_instantly and aid) else None,
        heat_map=an.company_heat_map(aid) if aid else [],
        gaps=an.gap_lists(aid) if aid else {"email_warm_not_li": [], "li_interested_not_email": []},
        lift=an.channel_lift(aid) if (g.has_instantly and g.has_heyreach and aid) else None,
        uni_funnel=an.unified_funnel(aid) if aid else [],
        icp=an.icp_learner(aid) if aid else None,
        exhaustion=an.lead_exhaustion(aid) if aid else None,
        timeframes=TIMEFRAMES, active_tf=active,
        start_str=request.args.get("start", ""),
        end_str=request.args.get("end", ""),
    )


@app.route("/deliverability")
@login_required
def deliverability():
    redir = _email_channel_required()
    if redir:
        return redir
    return render_template("deliverability.html",
        summary=an.deliverability_summary(account_id=g.account_id),
        campaigns=_cq().filter(Campaign.emails_sent_count > 0)
                  .order_by(Campaign.open_count_unique.desc()).all(),
    )


# ── Revenue ────────────────────────────────────────────────────────────────────

@app.route("/revenue")
@login_required
def revenue():
    redir = _email_channel_required()
    if redir:
        return redir
    aid = g.account_id
    start, end, active = _parse_range(request.args)
    return render_template("revenue.html",
        rev=an.revenue_summary(account_id=aid),
        funnel=an.funnel(start, end, account_id=aid),
        sentiment=an.sentiment_breakdown(account_id=aid),
        timeframes=TIMEFRAMES, active_tf=active,
        start_str=request.args.get("start", ""),
        end_str=request.args.get("end", ""),
    )


# ── Leads ──────────────────────────────────────────────────────────────────────

@app.route("/leads")
@login_required
def leads():
    redir = _email_channel_required()
    if redir:
        return redir
    ftype = request.args.get("f", "all")
    cid = request.args.get("cid")
    q = _pq()
    if cid:
        q = q.filter_by(campaign_id=cid)
    if ftype == "warm":
        q = q.filter(Prospect.email_open_count >= WARM_THRESHOLD)
    elif ftype == "hidden":
        q = q.filter(Prospect.email_open_count >= WARM_THRESHOLD,
                     Prospect.email_reply_count == 0)
    elif ftype == "replied":
        q = q.filter(Prospect.email_reply_count > 0)
    elif ftype == "interested":
        q = q.filter(Prospect.lt_interest_status == 1)
    leads_raw = q.all()
    if g.account_id:
        hr_map   = an._build_hr_map(g.account_id)
        def _key(p):
            return (an._norm(p.first_name or '') + an._norm(p.last_name or ''),
                    an._norm(p.company_name or ''))
        scored   = [(p, an.compute_lead_score(p, hr_map.get(_key(p)))) for p in leads_raw]
        scored.sort(key=lambda x: -x[1])
        leads    = [p for p, _ in scored[:500]]
        scores   = {p.id: s for p, s in scored}
        actions  = {}
        for p in leads:
            act = an._next_action(p, hr_map.get(_key(p)))
            if act:
                actions[p.id] = act
    else:
        leads_raw.sort(key=lambda p: -(p.warm_score or 0))
        leads   = leads_raw[:500]
        scores  = {}
        actions = {}
    return render_template("leads.html",
        leads=leads, ftype=ftype, cid=cid,
        campaigns=_cq().order_by(Campaign.name).all(),
        stages=PIPELINE_STAGES, dispositions=DISPOSITIONS,
        scores=scores, actions=actions, warm_threshold=WARM_THRESHOLD)


@app.route("/lead/<path:email>")
@login_required
def lead_detail(email):
    p = _pq().filter_by(email=email).first_or_404()
    c = db.session.get(Campaign, p.campaign_id) if p.campaign_id else None
    event_q = Event.query.filter(Event.prospect_email == email)
    if g.account_id:
        event_q = event_q.filter(
            db.or_(Event.account_id == g.account_id, Event.account_id.is_(None))
        )
    events = event_q.order_by(Event.occurred_at.desc()).all()
    return render_template("lead_detail.html",
        lead=p, campaign=c, events=events,
        stages=PIPELINE_STAGES, dispositions=DISPOSITIONS)


@app.route("/lead/<path:email>/update", methods=["POST"])
@login_required
def lead_update(email):
    p = _pq().filter_by(email=email).first_or_404()
    data = request.json or {}
    if "stage" in data:
        if p.stage != data["stage"]:
            p.stage = data["stage"]
            p.stage_changed_at = datetime.utcnow()
    if "notes" in data:
        p.notes = data["notes"]
    if "disposition" in data:
        p.disposition = data["disposition"]
    if "wake_date" in data:
        wd = data["wake_date"]
        p.wake_date = date.fromisoformat(wd) if wd else None
    db.session.commit()
    return jsonify({"ok": True})


# ── CRM pipeline ───────────────────────────────────────────────────────────────

@app.route("/crm")
@login_required
def crm():
    active_tab = request.args.get("tab", "linkedin" if (g.has_heyreach and not g.has_instantly) else "email")

    # Email pipeline
    email_pipeline = {s: [] for s in PIPELINE_STAGES}
    if g.has_instantly:
        for p in _pq().order_by(Prospect.warm_score.desc()).all():
            stage = p.stage if p.stage in PIPELINE_STAGES else "New"
            email_pipeline[stage].append(p)

    # LinkedIn pipeline
    li_pipeline = {s: [] for s in LI_PIPELINE_STAGES}
    if g.has_heyreach and g.account_id:
        for lead in (HeyReachLead.query
                     .filter_by(account_id=g.account_id)
                     .order_by(HeyReachLead.li_stage_changed_at.desc().nullslast(),
                               HeyReachLead.synced_at.desc())
                     .all()):
            stage = lead.li_stage if lead.li_stage in LI_PIPELINE_STAGES else "Contacted"
            li_pipeline[stage].append(lead)

    return render_template("crm.html",
        pipeline=email_pipeline, stages=PIPELINE_STAGES,
        li_pipeline=li_pipeline, li_stages=LI_PIPELINE_STAGES,
        active_tab=active_tab,
        now=datetime.utcnow())


@app.route("/crm/linkedin/<lid>/stage", methods=["POST"])
@login_required
def li_crm_move(lid):
    stage = request.form.get("stage", "")
    if stage not in LI_PIPELINE_STAGES:
        return ("Bad stage", 400)
    lead = HeyReachLead.query.filter_by(id=lid, account_id=g.account_id).first_or_404()
    lead.li_stage = stage
    lead.li_stage_changed_at = datetime.utcnow()
    db.session.commit()
    return ("", 204)


# ── Inbox ──────────────────────────────────────────────────────────────────────

def _inbox_threads():
    """Return a list of thread dicts sorted by latest message, inbound-first."""
    q = EmailMessage.query.filter(EmailMessage.ue_type == 2)
    if g.account_id:
        cids = [c.id for c in _cq().all()]
        if not cids:
            return []
        q = q.filter(EmailMessage.campaign_id.in_(cids))
    inbound = q.order_by(EmailMessage.timestamp_email.desc()).all()

    # Group into threads (one entry per unique thread_id, keeping latest)
    seen_threads = {}
    for msg in inbound:
        tid = msg.thread_id
        if tid not in seen_threads:
            p = None
            if msg.prospect_email:
                p = _pq().filter_by(email=msg.prospect_email).first()
            if not p and msg.lead_id:
                p = _pq().filter(Prospect.id == msg.lead_id).first()
            c = db.session.get(Campaign, msg.campaign_id) if msg.campaign_id else None
            seen_threads[tid] = {
                "thread_id": tid,
                "latest_msg": msg,
                "prospect": p,
                "campaign": c,
                "unread": msg.is_unread,
            }

    return list(seen_threads.values())


def _thread_messages(thread_id: str):
    """All messages in a thread, oldest-first, scoped to current account."""
    q = EmailMessage.query.filter_by(thread_id=thread_id)
    if g.account_id:
        cids = [c.id for c in _cq().all()]
        if cids:
            q = q.filter(EmailMessage.campaign_id.in_(cids))
        else:
            return []
    return q.order_by(EmailMessage.timestamp_email.asc()).all()


_OOO_RE = re.compile(
    r'out of (the )?office|auto.{0,4}reply|automatic reply|on (annual |maternity |paternity )?leave|'
    r'on vacation|away from (the )?office|will be back|i\'?m away|i am away|'
    r'not in (the )?office|currently away|noreply|no-reply|'
    r'not working today|next working day|working schedule|'
    r'i\'?ll? be back|will respond on my|return.{0,10}office',
    re.I
)


def _is_auto_reply(msg):
    if _OOO_RE.search(msg.from_address or ''):
        return True
    return bool(_OOO_RE.search(msg.body_text or ''))


def _action_queue():
    account_cids = None
    if g.account_id:
        account_cids = [c.id for c in _cq().all()]
        if not account_cids:
            return [], [], []

    # 1. Threads where the most recent message is an inbound reply (ue_type=2)
    base_q = (db.session.query(
        EmailMessage.thread_id,
        db.func.max(EmailMessage.timestamp_email).label('latest')
    ).filter(EmailMessage.timestamp_email.isnot(None)))
    if account_cids is not None:
        base_q = base_q.filter(EmailMessage.campaign_id.in_(account_cids))
    subq = base_q.group_by(EmailMessage.thread_id).subquery()

    unanswered_q = (db.session.query(EmailMessage)
        .join(subq, EmailMessage.thread_id == subq.c.thread_id)
        .filter(EmailMessage.timestamp_email == subq.c.latest)
        .filter(EmailMessage.ue_type == 2))
    if account_cids is not None:
        unanswered_q = unanswered_q.filter(EmailMessage.campaign_id.in_(account_cids))
    unanswered_msgs = unanswered_q.order_by(EmailMessage.timestamp_email.desc()).limit(100).all()

    unanswered = []
    for msg in unanswered_msgs:
        if _is_auto_reply(msg):
            continue
        p = None
        if msg.prospect_email:
            p = _pq().filter_by(email=msg.prospect_email).first()
        if not p and msg.lead_id:
            p = _pq().filter(Prospect.id == msg.lead_id).first()
        c = db.session.get(Campaign, msg.campaign_id) if msg.campaign_id else None
        unanswered.append({'msg': msg, 'prospect': p, 'campaign': c})

    # 2. Wake dates due today or overdue (not_now leads ready to re-engage)
    today = date.today()
    wake_prospects = (_pq()
        .filter(Prospect.wake_date.isnot(None))
        .filter(Prospect.wake_date <= today)
        .filter(Prospect.disposition == 'not_now')
        .filter(Prospect.do_not_contact == False)
        .order_by(Prospect.wake_date.asc())
        .all())
    wake = []
    for p in wake_prospects:
        c = db.session.get(Campaign, p.campaign_id) if p.campaign_id else None
        wake.append({
            'prospect': p,
            'campaign': c,
            'days_overdue': (today - p.wake_date).days,
        })

    # 3. Going cold: warm leads that haven't opened in 7+ days and never replied
    cold_cutoff = datetime.utcnow() - timedelta(days=7)
    cold_prospects = (_pq()
        .filter(Prospect.email_open_count >= WARM_THRESHOLD)
        .filter(Prospect.email_reply_count == 0)
        .filter(Prospect.timestamp_last_open.isnot(None))
        .filter(Prospect.timestamp_last_open <= cold_cutoff)
        .filter(Prospect.do_not_contact == False)
        .filter(Prospect.stage.notin_(['Won', 'Lost']))
        .order_by(Prospect.warm_score.desc())
        .limit(50)
        .all())
    cold = []
    for p in cold_prospects:
        c = db.session.get(Campaign, p.campaign_id) if p.campaign_id else None
        days_silent = (datetime.utcnow() - p.timestamp_last_open).days
        cold.append({'prospect': p, 'campaign': c, 'days_silent': days_silent})

    return unanswered, wake, cold


@app.route('/crm/queue')
@login_required
def crm_queue():
    redir = _email_channel_required()
    if redir:
        return redir
    unanswered, wake, cold = _action_queue()
    total = len(unanswered) + len(wake) + len(cold)
    return render_template('crm_queue.html',
        unanswered=unanswered, wake=wake, cold=cold,
        total=total, today=date.today())


@app.route("/inbox")
@login_required
def inbox():
    redir = _email_channel_required()
    if redir:
        return redir
    threads = _inbox_threads()
    active_thread_id = request.args.get("t")
    active_thread = None
    messages = []
    prospect = None
    campaign = None

    if active_thread_id:
        messages = _thread_messages(active_thread_id)
        # Find corresponding thread entry
        for th in threads:
            if th["thread_id"] == active_thread_id:
                active_thread = th
                prospect = th["prospect"]
                campaign = th["campaign"]
                break
        # Prospect/campaign fallback if not in inbound list (thread only has outbound)
        if not prospect and messages:
            m = next((x for x in messages if x.ue_type == 2), messages[0])
            if m.prospect_email:
                prospect = _pq().filter_by(email=m.prospect_email).first()
            if not prospect and m.lead_id:
                prospect = _pq().filter(Prospect.id == m.lead_id).first()
            if not campaign and m.campaign_id:
                campaign = db.session.get(Campaign, m.campaign_id)

    unread_count = sum(1 for t in threads if t["unread"])

    return render_template("inbox.html",
        threads=threads,
        active_thread_id=active_thread_id,
        messages=messages,
        prospect=prospect,
        campaign=campaign,
        unread_count=unread_count,
        dispositions=DISPOSITIONS,
        stages=PIPELINE_STAGES,
    )


@app.route("/inbox/email/<email_id>/body")
@login_required
def email_body(email_id):
    """Serve a raw email HTML body for iframe rendering."""
    from flask import Response, abort
    msg = db.get_or_404(EmailMessage, email_id)
    # Account scoping: verify this message belongs to the current account
    if g.account_id:
        if msg.campaign_id:
            c = db.session.get(Campaign, msg.campaign_id)
            if c and c.account_id and c.account_id != g.account_id:
                abort(403)
        elif msg.prospect_email:
            if not _pq().filter_by(email=msg.prospect_email).first():
                abort(403)
    body = msg.body_html or f"<p style='font-family:sans-serif;color:#555'>{html.escape(msg.subject or '(empty)')}</p>"
    page = f"""<!doctype html><html><head>
<meta charset="utf-8">
<style>
  body{{margin:0;padding:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:14px;line-height:1.6;color:#171a23;background:#fff}}
  a{{color:#5b54f0}}
  img{{max-width:100%;height:auto}}
  blockquote{{border-left:3px solid #e7e9f0;margin:0 0 0 8px;padding-left:12px;color:#6b7280}}
</style></head><body>{body}</body></html>"""
    return Response(page, content_type="text/html")


@app.route("/inbox/reply", methods=["POST"])
@login_required
def inbox_reply():
    data = request.json or {}
    reply_to_id = data.get("email_id")
    body = data.get("body", "").strip()
    eaccount = data.get("eaccount", "")
    prospect_email = data.get("prospect_email", "")
    disposition = data.get("disposition")
    thread_id = data.get("thread_id")

    if not reply_to_id or not body:
        return jsonify({"ok": False, "msg": "email_id and body are required"})

    client = _client()
    if not client:
        return jsonify({"ok": False, "msg": "No API key configured"})

    # Find the original email to get the subject
    original = db.session.get(EmailMessage, reply_to_id)
    subject = ""
    if original:
        subject = original.subject
        if not subject.lower().startswith("re:"):
            subject = f"Re: {subject}"
        if not eaccount and original.eaccount:
            eaccount = original.eaccount

    try:
        client.send_reply(reply_to_id, eaccount, body, subject)
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)})

    # Update prospect disposition + stage if provided
    if prospect_email and disposition:
        p = _pq().filter_by(email=prospect_email).first()
        if p:
            p.disposition = disposition
            if disposition == "interested" and p.stage not in ("Meeting", "Won"):
                p.stage = "Replied"
                p.stage_changed_at = datetime.utcnow()
            elif disposition == "unsubscribed":
                p.do_not_contact = True
            db.session.commit()

    return jsonify({"ok": True, "msg": "Reply sent"})


@app.route("/inbox/disposition", methods=["POST"])
@login_required
def inbox_disposition():
    """Quick disposition update from the inbox without sending a reply."""
    data = request.json or {}
    prospect_email = data.get("prospect_email")
    disposition = data.get("disposition")
    if not prospect_email or not disposition:
        return jsonify({"ok": False, "msg": "prospect_email and disposition required"})

    p = _pq().filter_by(email=prospect_email).first()
    if not p:
        return jsonify({"ok": False, "msg": "Prospect not found"})

    p.disposition = disposition
    if disposition == "interested" and p.stage not in ("Meeting", "Won"):
        p.stage = "Replied"
        p.stage_changed_at = datetime.utcnow()
    elif disposition == "unsubscribed":
        p.do_not_contact = True
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/crm/queue/action", methods=["POST"])
@login_required
def crm_queue_action():
    data = request.json or {}
    email = data.get("email")
    if not email:
        return jsonify({"ok": False, "msg": "email required"})
    p = _pq().filter_by(email=email).first()
    if not p:
        return jsonify({"ok": False, "msg": "Prospect not found"})

    if "wake_days" in data:
        p.wake_date = date.today() + timedelta(days=int(data["wake_days"]))
        p.disposition = "not_now"

    if "note" in data:
        note = data["note"].strip()
        if note:
            stamp = datetime.utcnow().strftime("%b %d")
            p.notes = (p.notes.rstrip() + f"\n\n[{stamp}] {note}") if p.notes else f"[{stamp}] {note}"

    if "disposition" in data:
        p.disposition = data["disposition"]
        if data["disposition"] == "interested" and p.stage not in ("Meeting", "Won"):
            p.stage = "Replied"
            p.stage_changed_at = datetime.utcnow()
        elif data["disposition"] == "unsubscribed":
            p.do_not_contact = True

    db.session.commit()
    return jsonify({"ok": True})


# ── Settings ───────────────────────────────────────────────────────────────────

@app.route("/settings")
@login_required
def settings():
    key = os.getenv("INSTANTLY_API_KEY", "").strip()
    preview = (key[:6] + "…" + key[-4:]) if len(key) > 12 else ("set" if key else "not set")
    last = db.session.query(db.func.max(Campaign.synced_at)).scalar()
    return render_template("settings.html", preview=preview, last_sync=last)


@app.route("/settings/test", methods=["POST"])
@login_required
def test_connection():
    client = _client()
    if not client:
        return jsonify({"ok": False, "msg": "No API key in .env"})
    return jsonify({"ok": client.test_connection(),
                    "msg": "Connected!" if client.test_connection() else "Failed: check key."})


# ── Search ─────────────────────────────────────────────────────────────────────

@app.route("/api/search")
@login_required
def api_search():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify({"prospects": [], "campaigns": []})
    term = f"%{q}%"
    prospects = (_pq().filter(
        db.or_(
            Prospect.first_name.ilike(term),
            Prospect.last_name.ilike(term),
            Prospect.email.ilike(term),
            Prospect.company_name.ilike(term),
            Prospect.job_title.ilike(term),
        )
    ).order_by(Prospect.warm_score.desc()).limit(8).all())
    campaigns = _cq().filter(Campaign.name.ilike(term)).limit(4).all()
    return jsonify({
        "prospects": [{"name": p.full_name, "email": p.email, "company": p.company_name,
                       "url": url_for("lead_detail", email=p.email)} for p in prospects],
        "campaigns": [{"name": c.name,
                       "url": url_for("campaign_detail", cid=c.id)} for c in campaigns],
    })


@app.route("/search")
@login_required
def search():
    q = request.args.get("q", "").strip()
    prospects, campaigns = [], []
    if len(q) >= 2:
        term = f"%{q}%"
        prospects = (_pq().filter(
            db.or_(
                Prospect.first_name.ilike(term),
                Prospect.last_name.ilike(term),
                Prospect.email.ilike(term),
                Prospect.company_name.ilike(term),
                Prospect.job_title.ilike(term),
            )
        ).order_by(Prospect.warm_score.desc()).limit(200).all())
        campaigns = _cq().filter(Campaign.name.ilike(term)).all()
    return render_template("search.html", q=q, prospects=prospects, campaigns=campaigns)


# ── Leads bulk actions + CSV export ────────────────────────────────────────────

@app.route("/leads/bulk-update", methods=["POST"])
@login_required
def leads_bulk_update():
    data = request.json or {}
    emails = data.get("emails", [])
    if not emails:
        return jsonify({"ok": False, "msg": "No leads selected"})
    prospects = _pq().filter(Prospect.email.in_(emails)).all()
    updated = 0
    for p in prospects:
        if "stage" in data and data["stage"] and p.stage != data["stage"]:
            p.stage = data["stage"]
            p.stage_changed_at = datetime.utcnow()
            updated += 1
        if "disposition" in data and data["disposition"]:
            p.disposition = data["disposition"]
            if data["disposition"] == "unsubscribed":
                p.do_not_contact = True
            updated += 1
    db.session.commit()
    return jsonify({"ok": True, "updated": len(prospects)})


@app.route("/leads/export.csv")
@login_required
def leads_export():
    ftype = request.args.get("f", "all")
    cid = request.args.get("cid")
    emails = request.args.getlist("emails")
    q = _pq()
    if emails:
        q = q.filter(Prospect.email.in_(emails))
    else:
        if cid:
            q = q.filter_by(campaign_id=cid)
        if ftype == "warm":
            q = q.filter(Prospect.email_open_count >= WARM_THRESHOLD)
        elif ftype == "replied":
            q = q.filter(Prospect.email_reply_count > 0)
        elif ftype == "hidden":
            q = q.filter(Prospect.email_open_count >= WARM_THRESHOLD,
                         Prospect.email_reply_count == 0)
        elif ftype == "interested":
            q = q.filter(Prospect.lt_interest_status == 1)
    leads = q.order_by(Prospect.warm_score.desc()).all()
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Name", "Email", "Company", "Job Title", "Stage", "Disposition",
                "Opens", "Clicks", "Replies", "Warm Score", "Wake Date", "Notes"])
    for p in leads:
        w.writerow([p.full_name, p.email, p.company_name or "", p.job_title or "",
                    p.stage or "", p.disposition or "", p.email_open_count,
                    p.email_click_count, p.email_reply_count, p.warm_score,
                    p.wake_date.isoformat() if p.wake_date else "",
                    (p.notes or "").replace("\n", " ")])
    filename = f"leads_{ftype}.csv"
    return (out.getvalue(),
            200,
            {"Content-Type": "text/csv",
             "Content-Disposition": f"attachment; filename={filename}"})


# ── JSON APIs for charts ───────────────────────────────────────────────────────

@app.route("/api/daily")
@login_required
def api_daily():
    return jsonify(an.daily_series(
        request.args.get("cid"),
        int(request.args.get("days", 30)),
        account_id=g.account_id,
    ))


# ── Login / Logout ─────────────────────────────────────────────────────────────

@app.route("/login", methods=["GET", "POST"])
def login_page():
    if 'user_id' in session:
        return redirect(url_for('dashboard'))
    error = None
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        user = AppUser.query.filter_by(email=email, is_active=True).first()
        if user and check_password_hash(user.password_hash, password):
            session.clear()
            session['user_id'] = user.id
            session['role'] = user.role
            if user.role == 'superadmin':
                first_acct = InstantlyAccount.query.filter_by(is_active=True).first()
                session['active_account_id'] = first_acct.id if first_acct else None
            user.last_login_at = datetime.utcnow()
            db.session.commit()
            return redirect(url_for('dashboard'))
        error = "Invalid email or password."
    return render_template("login.html", error=error)


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for('login_page'))


# ── Admin ──────────────────────────────────────────────────────────────────────

@app.route("/admin")
@superadmin_required
def admin():
    users = AppUser.query.order_by(AppUser.created_at.asc()).all()
    accounts = InstantlyAccount.query.order_by(InstantlyAccount.created_at.asc()).all()
    tokens = ClientToken.query.order_by(ClientToken.created_at.desc()).all()
    client_activity = (ClientActivity.query
                       .order_by(ClientActivity.created_at.desc())
                       .limit(50).all())
    # Build per-client HeyReach mapping (prefer active record)
    hr_by_account = {}
    for h in HeyReachAccount.query.order_by(HeyReachAccount.is_active.desc(),
                                             HeyReachAccount.created_at.asc()).all():
        if h.account_id not in hr_by_account:
            hr_by_account[h.account_id] = h
    # Build per-client Calendly mapping
    cal_by_account = {}
    for c in CalendlyAccount.query.filter_by(is_active=True).all():
        cal_by_account[c.account_id] = c
    tab = request.args.get("tab", "users")
    return render_template("admin.html", users=users, accounts=accounts,
                           tokens=tokens, client_activity=client_activity,
                           hr_by_account=hr_by_account,
                           cal_by_account=cal_by_account,
                           active_tab=tab)


@app.route("/admin/users/add", methods=["POST"])
@superadmin_required
def admin_user_add():
    name = request.form.get("name", "").strip()
    email = request.form.get("email", "").strip().lower()
    password = request.form.get("password", "").strip()
    role = request.form.get("role", "user")
    raw_ids = request.form.getlist("account_ids")  # multi-select
    allowed_ids = [int(x) for x in raw_ids if x.strip()]
    if not name or not email or not password:
        flash("Name, email and password are required.", "error")
        return redirect(url_for("admin"))
    if len(password) < 6:
        flash("Password must be at least 6 characters.", "error")
        return redirect(url_for("admin"))
    if AppUser.query.filter_by(email=email).first():
        flash(f"Email already exists.", "error")
        return redirect(url_for("admin"))
    if role != 'superadmin' and not allowed_ids:
        flash("Assign at least one account to this user.", "error")
        return redirect(url_for("admin"))
    user = AppUser(
        name=name, email=email,
        password_hash=generate_password_hash(password),
        role=role,
        account_id=allowed_ids[0] if allowed_ids and role != 'superadmin' else None,
        account_ids=json.dumps(allowed_ids) if role != 'superadmin' else "[]",
    )
    db.session.add(user)
    db.session.commit()
    flash(f"User {email} created.", "success")
    return redirect(url_for("admin"))


@app.route("/admin/users/<int:uid>/toggle", methods=["POST"])
@superadmin_required
def admin_user_toggle(uid):
    user = AppUser.query.get_or_404(uid)
    if user.id == session['user_id']:
        flash("Cannot deactivate your own account.", "error")
        return redirect(url_for("admin"))
    user.is_active = not user.is_active
    db.session.commit()
    flash(f"User {'activated' if user.is_active else 'deactivated'}.", "success")
    return redirect(url_for("admin"))


@app.route("/admin/users/<int:uid>/reset", methods=["POST"])
@superadmin_required
def admin_user_reset(uid):
    user = AppUser.query.get_or_404(uid)
    pw = request.form.get("password", "").strip()
    if not pw or len(pw) < 6:
        flash("Password must be at least 6 characters.", "error")
        return redirect(url_for("admin"))
    user.password_hash = generate_password_hash(pw)
    db.session.commit()
    flash(f"Password reset for {user.email}.", "success")
    return redirect(url_for("admin"))


@app.route("/admin/accounts/add", methods=["POST"])
@superadmin_required
def admin_account_add():
    name = request.form.get("name", "").strip()
    api_key = request.form.get("api_key", "").strip()
    heyreach_key = request.form.get("heyreach_key", "").strip()
    heyreach_name = request.form.get("heyreach_name", "HeyReach").strip()
    calendly_secret = request.form.get("calendly_token", "").strip()
    if not name:
        flash("Client name is required.", "error")
        return redirect(url_for("admin", tab="clients"))
    if not api_key and not heyreach_key:
        flash("At least one outreach channel (Instantly or HeyReach) must be configured.", "error")
        return redirect(url_for("admin", tab="clients"))
    acct = InstantlyAccount(name=name, api_key=api_key or "")
    db.session.add(acct)
    db.session.flush()  # get acct.id before creating linked records
    if heyreach_key:
        try:
            ok = HeyReachClient(heyreach_key).check_key()
            if not ok:
                db.session.rollback()
                flash("HeyReach API key is invalid.", "error")
                return redirect(url_for("admin", tab="clients"))
        except Exception as e:
            db.session.rollback()
            flash(f"Could not verify HeyReach key: {e}", "error")
            return redirect(url_for("admin", tab="clients"))
        hr = HeyReachAccount(name=heyreach_name or name, api_key=heyreach_key,
                             account_id=acct.id)
        db.session.add(hr)
    if calendly_secret:
        try:
            _cl = CalendlyClient(calendly_secret)
            _me = _cl.get_current_user()
            cal = CalendlyAccount(api_token=calendly_secret,
                                  user_uri=_me.get("uri", ""),
                                  organization_uri=_me.get("current_organization", ""),
                                  account_id=acct.id, created_at=datetime.utcnow())
            db.session.add(cal)
        except CalendlyError:
            flash("Calendly token invalid -- client added without Calendly.", "error")
    db.session.commit()
    channels = []
    if api_key:
        channels.append("email (Instantly)")
    if heyreach_key:
        channels.append("LinkedIn (HeyReach)")
    if calendly_secret:
        channels.append("Calendly inbound")
    flash(f"Client '{name}' added with {' + '.join(channels)}. Sync to pull data.", "success")
    return redirect(url_for("admin", tab="clients"))


@app.route("/admin/clients/<int:cid>/add-instantly", methods=["POST"])
@superadmin_required
def admin_client_add_instantly(cid):
    acct = InstantlyAccount.query.get_or_404(cid)
    api_key = request.form.get("api_key", "").strip()
    if not api_key:
        flash("Instantly API key is required.", "error")
        return redirect(url_for("admin", tab="clients"))
    acct.api_key = api_key
    db.session.commit()
    flash(f"Email (Instantly) connected for '{acct.name}'. Click Sync Email to pull data.", "success")
    return redirect(url_for("admin", tab="clients"))


@app.route("/admin/accounts/<int:aid>/toggle", methods=["POST"])
@superadmin_required
def admin_account_toggle(aid):
    acct = InstantlyAccount.query.get_or_404(aid)
    acct.is_active = not acct.is_active
    db.session.commit()
    flash(f"Account {'activated' if acct.is_active else 'deactivated'}.", "success")
    return redirect(url_for("admin", tab="clients"))


@app.route("/admin/accounts/<int:aid>/sync", methods=["POST"])
@superadmin_required
def admin_account_sync(aid):
    acct = InstantlyAccount.query.get_or_404(aid)
    client = InstantlyClient(acct.api_key.strip())
    try:
        s = run_sync(client, account_id=aid)
        acct.last_synced_at = datetime.utcnow()
        if s.get("workspace_name"):
            acct.workspace_name = s["workspace_name"]
        db.session.commit()
        flash(f"Synced '{acct.name}': {s['campaigns']} campaigns, {s['leads']} leads.", "success")
    except Exception as e:
        flash(f"Sync failed: {e}", "error")
    return redirect(url_for("admin", tab="clients"))


@app.route("/admin/heyreach/add", methods=["POST"])
@superadmin_required
def admin_heyreach_add():
    name = request.form.get("name", "HeyReach").strip()
    api_key = request.form.get("api_key", "").strip()
    account_id = request.form.get("account_id", "").strip()
    if not api_key or not account_id:
        flash("API key and account are required.", "error")
        return redirect(url_for("admin", tab="clients"))
    try:
        account_id = int(account_id)
    except ValueError:
        flash("Invalid account.", "error")
        return redirect(url_for("admin", tab="clients"))
    # Verify key
    try:
        ok = HeyReachClient(api_key).check_key()
        if not ok:
            flash("HeyReach API key is invalid.", "error")
            return redirect(url_for("admin", tab="clients"))
    except Exception as e:
        flash(f"Could not verify HeyReach key: {e}", "error")
        return redirect(url_for("admin", tab="clients"))
    hr = HeyReachAccount(name=name, api_key=api_key, account_id=account_id)
    db.session.add(hr)
    db.session.commit()
    flash(f"HeyReach account '{name}' added. Sync it to pull LinkedIn data.", "success")
    return redirect(url_for("admin", tab="clients"))


@app.route("/admin/heyreach/<int:hid>/sync", methods=["POST"])
@superadmin_required
def admin_heyreach_sync(hid):
    hr = HeyReachAccount.query.get_or_404(hid)
    try:
        s = run_heyreach_sync(hr)
        flash(f"HeyReach synced: {s['campaigns']} campaigns, {s['leads']} leads.", "success")
    except Exception as e:
        flash(f"HeyReach sync failed: {e}", "error")
    return redirect(url_for("admin", tab="clients"))


@app.route("/admin/heyreach/<int:hid>/toggle", methods=["POST"])
@superadmin_required
def admin_heyreach_toggle(hid):
    hr = HeyReachAccount.query.get_or_404(hid)
    hr.is_active = not hr.is_active
    db.session.commit()
    flash(f"HeyReach account {'activated' if hr.is_active else 'deactivated'}.", "success")
    return redirect(url_for("admin", tab="clients"))


@app.route("/admin/calendly/add", methods=["POST"])
@superadmin_required
def admin_calendly_add():
    account_id = request.form.get("account_id", "").strip()
    api_token = request.form.get("api_token", "").strip()
    if not api_token or not account_id:
        flash("Account and personal access token are required.", "error")
        return redirect(url_for("admin", tab="clients"))
    try:
        account_id = int(account_id)
    except ValueError:
        flash("Invalid account.", "error")
        return redirect(url_for("admin", tab="clients"))
    # Verify token and fetch user/org URIs
    try:
        client = CalendlyClient(api_token)
        me = client.get_current_user()
        user_uri = me.get("uri", "")
        org_uri = me.get("current_organization", "")
    except CalendlyError as e:
        flash(f"Calendly token invalid: {e}", "error")
        return redirect(url_for("admin", tab="clients"))
    CalendlyAccount.query.filter_by(account_id=account_id).delete()
    cal = CalendlyAccount(api_token=api_token, user_uri=user_uri,
                          organization_uri=org_uri, account_id=account_id,
                          created_at=datetime.utcnow())
    db.session.add(cal)
    db.session.commit()
    flash("Calendly connected. Click Sync Calendly to pull bookings.", "success")
    return redirect(url_for("admin", tab="clients"))


@app.route("/admin/calendly/<int:cid>/sync", methods=["POST"])
@superadmin_required
def admin_calendly_sync(cid):
    cal = CalendlyAccount.query.get_or_404(cid)
    try:
        s = run_calendly_sync(cal)
        flash(f"Calendly synced: {s['new_leads']} new leads, {s['updated']} updated"
              f" from {s['bookings']} bookings.", "success")
    except Exception as e:
        flash(f"Calendly sync failed: {e}", "error")
    return redirect(url_for("admin", tab="clients"))


@app.route("/admin/calendly/<int:cid>/remove", methods=["POST"])
@superadmin_required
def admin_calendly_remove(cid):
    cal = CalendlyAccount.query.get_or_404(cid)
    aid = cal.account_id
    db.session.delete(cal)
    db.session.commit()
    flash("Calendly disconnected.", "success")
    return redirect(url_for("admin", tab="clients"))


@app.route("/switch-account", methods=["POST"])
@login_required
def admin_switch_account():
    aid_str = request.form.get("account_id")
    if not aid_str:
        return redirect(url_for("dashboard"))

    aid = int(aid_str)

    # Validate the user is allowed to access this account
    allowed_ids = [a.id for a in g.user_accounts]
    if aid not in allowed_ids:
        flash("Account not found.", "error")
        return redirect(url_for("dashboard"))

    session['active_account_id'] = aid
    acct = db.session.get(InstantlyAccount, aid)

    # Auto-sync if this account has never been synced
    if acct and not acct.last_synced_at:
        flash(f"Switched to '{acct.name}'. First-time sync running in the background — refresh in ~30 seconds.", "success")
        def do_sync(app_ctx, account_id, api_key):
            with app_ctx:
                try:
                    c = InstantlyClient(api_key)
                    s = run_sync(c, account_id=account_id)
                    a = db.session.get(InstantlyAccount, account_id)
                    if a:
                        a.last_synced_at = datetime.utcnow()
                        if s.get("workspace_name"):
                            a.workspace_name = s["workspace_name"]
                        db.session.commit()
                    log.info(f"Auto-sync on switch: account {account_id} done — {s['leads']} leads")
                except Exception as e:
                    log.warning(f"Auto-sync on switch failed: {e}")
        threading.Thread(
            target=do_sync,
            args=(app.app_context(), aid, acct.api_key.strip()),
            daemon=True
        ).start()
    else:
        flash(f"Switched to '{acct.name}'.", "success")

    return redirect(url_for("dashboard"))


def _auto_sync_loop(interval_secs):
    import time
    time.sleep(90)
    while True:
        try:
            with app.app_context():
                accounts = InstantlyAccount.query.filter_by(is_active=True).all()
                if accounts:
                    for acct in accounts:
                        c = InstantlyClient(acct.api_key.strip())
                        run_sync(c, account_id=acct.id)
                        acct.last_synced_at = datetime.utcnow()
                    db.session.commit()
                    log.info(f"Auto-sync: {len(accounts)} account(s) done")
                else:
                    c = _client()
                    if c:
                        run_sync(c)
        except Exception as e:
            log.warning(f"Auto-sync failed: {e}")
        time.sleep(interval_secs)


# ── Client dashboard (token-based, no login) ───────────────────────────────────

def _get_client_token(token_str):
    """Validate a client token. Returns ClientToken or aborts 404."""
    from flask import abort
    ct = ClientToken.query.filter_by(token=token_str).first()
    if not ct or not ct.is_valid:
        abort(404)
    return ct


def _client_prospect_q(account_id):
    return Prospect.query.filter(Prospect.account_id == account_id)


def _client_campaign_q(account_id):
    return Campaign.query.filter(Campaign.account_id == account_id)


@app.route("/client/<token_str>")
def client_overview(token_str):
    ct = _get_client_token(token_str)
    aid = ct.account_id
    acct = db.session.get(InstantlyAccount, aid)

    start, end, active = _parse_range(request.args)

    # Email KPIs (all-time from Campaign aggregates)
    from sqlalchemy import func as sqlfunc
    row = (db.session.query(
        sqlfunc.sum(Campaign.emails_sent_count),
        sqlfunc.sum(Campaign.contacted_count),
        sqlfunc.sum(Campaign.open_count_unique),
        sqlfunc.sum(Campaign.reply_count_unique),
        sqlfunc.sum(Campaign.total_interested),
        sqlfunc.sum(Campaign.total_meeting_booked),
    ).filter(Campaign.account_id == aid).first())
    sent, contacted, opens_u, replies_u, interested_ct, meetings_ct = [int(x or 0) for x in (row or (0,)*6)]
    life = {
        "sent": sent, "opens": opens_u,
        "open_rate": round(opens_u / contacted * 100, 1) if contacted else 0.0,
        "replies": replies_u,
        "reply_rate": round(replies_u / contacted * 100, 1) if contacted else 0.0,
        "interested": interested_ct, "meetings": meetings_ct,
    }

    # LinkedIn KPIs (if LinkedIn is configured for this account)
    has_li = HeyReachAccount.query.filter_by(account_id=aid, is_active=True).first() is not None
    li_kpis = an.heyreach_kpis(aid) if has_li else None

    # Funnel: unified (both channels) if LinkedIn available, else email-only
    funnel_stages = an.unified_funnel(aid) if has_li else an.funnel(start, end, account_id=aid)

    # Daily trend: use selected range or default to 30 days
    if start:
        trend = an.daily_series(start=start, end=end, account_id=aid)
    else:
        trend = an.daily_series(days=30, account_id=aid)

    # Pipeline counts
    pq = _client_prospect_q(aid)
    pipeline = {
        "meeting":    pq.filter(Prospect.stage == "Meeting").count(),
        "interested": pq.filter(Prospect.lt_interest_status == 1).count(),
        "won":        pq.filter(Prospect.stage == "Won").count(),
    }

    # Top leads by score
    all_prospects = pq.all()
    hr_map = an._build_hr_map(aid)
    def _ckey(p):
        return (an._norm(p.first_name or '') + an._norm(p.last_name or ''),
                an._norm(p.company_name or ''))
    scored = [(p, an.compute_lead_score(p, hr_map.get(_ckey(p)))) for p in all_prospects]
    scored.sort(key=lambda x: -x[1])
    top_leads = [(p, s) for p, s in scored[:10] if s >= 10]

    campaigns = (_client_campaign_q(aid)
                 .filter(Campaign.emails_sent_count > 0)
                 .order_by(Campaign.emails_sent_count.desc()).all())

    # Week-over-week deltas
    wk = an.weekly_kpis(aid)

    # Recent wins feed
    wins = an.recent_wins(aid)

    # Funnel with conversion rates between each consecutive stage
    funnel_with_rates = []
    for i, stage in enumerate(funnel_stages):
        prev_val = funnel_stages[i - 1]["value"] if i > 0 else None
        conv = round(stage["value"] / prev_val * 100, 1) if prev_val else None
        funnel_with_rates.append({**stage, "conv_rate": conv})

    # Pipeline forecast using historical reply-to-meeting rate
    hist_rate = (life["meetings"] / life["replies"]) if (life.get("replies") and life.get("meetings")) else 0.35
    forecast = {
        "interested": pipeline["interested"],
        "active_meetings": pipeline["meeting"],
        "rate_pct": round(hist_rate * 100),
        "min": max(1, int(pipeline["interested"] * hist_rate * 0.7)),
        "max": int(pipeline["interested"] * hist_rate * 1.3) + 1,
    } if pipeline["interested"] > 0 else None

    return render_template("client_overview.html",
        ct=ct, acct=acct, life=life, has_li=has_li, li_kpis=li_kpis,
        funnel=funnel_with_rates, trend=trend, pipeline=pipeline,
        top_leads=top_leads, campaigns=campaigns,
        wk=wk, wins=wins, forecast=forecast,
        timeframes=TIMEFRAMES, active_tf=active,
        start_str=request.args.get("start", ""),
        end_str=request.args.get("end", ""),
    )


@app.route("/client/<token_str>/intelligence")
def client_intelligence(token_str):
    ct = _get_client_token(token_str)
    aid = ct.account_id
    acct = db.session.get(InstantlyAccount, aid)
    return render_template("client_intelligence.html",
        ct=ct, acct=acct,
        heat_map=an.company_heat_map(aid),
        icp=an.icp_learner(aid),
        exhaustion=an.lead_exhaustion(aid),
        gaps=an.gap_lists(aid),
    )


@app.route("/client/<token_str>/leads")
def client_leads(token_str):
    ct = _get_client_token(token_str)
    aid = ct.account_id
    acct = db.session.get(InstantlyAccount, aid)

    filt = request.args.get("f", "replied")
    cid = request.args.get("cid")

    q = _client_prospect_q(aid)
    if cid:
        q = q.filter(Prospect.campaign_id == cid)

    if filt == "interested":
        q = q.filter(Prospect.lt_interest_status == 1)
    elif filt == "meeting":
        q = q.filter(Prospect.stage == "Meeting")
    elif filt == "won":
        q = q.filter(Prospect.stage == "Won")
    elif filt == "warm":
        q = q.filter(Prospect.email_open_count >= WARM_THRESHOLD,
                     Prospect.email_reply_count == 0)
    else:  # "replied" default
        q = q.filter(Prospect.email_reply_count > 0)

    leads = q.order_by(Prospect.timestamp_last_reply.desc()).limit(300).all()
    campaigns = _client_campaign_q(aid).order_by(Campaign.name).all()
    campaign_map = {c.id: c.name for c in campaigns}

    # Resolve open_lead for thread panel (need full object for note/outcome forms)
    open_email = request.args.get("open", "").strip().lower()
    open_lead = None
    if open_email:
        open_lead = _client_prospect_q(aid).filter_by(email=open_email).first()

    return render_template("client_leads.html",
        ct=ct, acct=acct, leads=leads, filt=filt, cid=cid,
        campaigns=campaigns, campaign_map=campaign_map,
        open_lead=open_lead,
    )


@app.route("/client/<token_str>/thread/<thread_id>")
def client_thread(token_str, thread_id):
    ct = _get_client_token(token_str)
    aid = ct.account_id
    acct = db.session.get(InstantlyAccount, aid)

    # Validate thread belongs to this account's campaigns
    account_cids = [c.id for c in _client_campaign_q(aid).all()]
    msgs = (EmailMessage.query
            .filter_by(thread_id=thread_id)
            .filter(EmailMessage.campaign_id.in_(account_cids))
            .order_by(EmailMessage.timestamp_email.asc()).all())
    if not msgs:
        from flask import abort
        abort(404)

    prospect = None
    campaign = None
    m0 = next((m for m in msgs if m.ue_type == 2), msgs[0])
    if m0.prospect_email:
        prospect = _client_prospect_q(aid).filter_by(email=m0.prospect_email).first()
    if not prospect and m0.lead_id:
        prospect = (_client_prospect_q(aid)
                    .filter(Prospect.id == m0.lead_id).first())
    if m0.campaign_id:
        campaign = db.session.get(Campaign, m0.campaign_id)

    return render_template("client_thread.html",
        ct=ct, acct=acct, messages=msgs,
        prospect=prospect, campaign=campaign,
    )


# ── Admin: client token management ─────────────────────────────────────────────

@app.route("/client/<token_str>/lead/<prospect_id>/outcome", methods=["POST"])
def client_lead_outcome(token_str, prospect_id):
    ct = _get_client_token(token_str)
    aid = ct.account_id
    p = _client_prospect_q(aid).filter(Prospect.id == prospect_id).first_or_404()

    outcome = request.form.get("outcome", "").strip()   # won | lost | reschedule
    note_text = request.form.get("note", "").strip()[:1000]

    if outcome not in ("won", "lost", "reschedule"):
        flash("Invalid outcome.", "error")
        return redirect(url_for("client_leads", token_str=token_str, f="meeting"))

    if outcome == "won":
        p.stage = "Won"
        p.stage_changed_at = datetime.utcnow()
        action = "outcome_won"
        value = "Won"
    elif outcome == "lost":
        p.stage = "Lost"
        p.stage_changed_at = datetime.utcnow()
        p.disposition = "not_interested"
        action = "outcome_lost"
        value = "Lost"
    else:  # reschedule
        # stays in Meeting — just log it and save note
        action = "outcome_reschedule"
        value = "Reschedule"

    # Tag the note as client-submitted so it's distinguishable internally
    if note_text:
        ts = datetime.utcnow().strftime("%-d %b %Y")
        tagged = f"[Client, {ts}]: {note_text}"
        p.notes = (p.notes + "\n\n" + tagged).strip() if p.notes else tagged

    act = ClientActivity(
        token_id=ct.id, account_id=aid,
        prospect_id=p.id, prospect_email=p.email,
        action=action, value=value, note=note_text,
    )
    db.session.add(act)
    db.session.commit()
    return redirect(url_for("client_leads", token_str=token_str, f="meeting",
                            _anchor="outcome-saved"))


@app.route("/client/<token_str>/lead/<prospect_id>/note", methods=["POST"])
def client_lead_note(token_str, prospect_id):
    ct = _get_client_token(token_str)
    aid = ct.account_id
    p = _client_prospect_q(aid).filter(Prospect.id == prospect_id).first_or_404()

    note_text = request.form.get("note", "").strip()[:1000]
    if not note_text:
        return redirect(request.referrer or url_for("client_leads", token_str=token_str))

    ts = datetime.utcnow().strftime("%-d %b %Y")
    tagged = f"[Client, {ts}]: {note_text}"
    p.notes = (p.notes + "\n\n" + tagged).strip() if p.notes else tagged

    act = ClientActivity(
        token_id=ct.id, account_id=aid,
        prospect_id=p.id, prospect_email=p.email,
        action="note_added", value="", note=note_text,
    )
    db.session.add(act)
    db.session.commit()
    return redirect(request.referrer or url_for("client_leads", token_str=token_str))


@app.route("/client/<token_str>/thread-by-email")
def client_thread_by_email(token_str):
    ct = _get_client_token(token_str)
    aid = ct.account_id
    email = request.args.get("email", "").strip().lower()
    if not email:
        return jsonify({"messages": []})

    account_cids = [c.id for c in _client_campaign_q(aid).all()]
    if not account_cids:
        return jsonify({"messages": []})

    # Find the most recent thread for this prospect in this account
    latest_msg = (EmailMessage.query
        .filter(EmailMessage.prospect_email == email)
        .filter(EmailMessage.campaign_id.in_(account_cids))
        .order_by(EmailMessage.timestamp_email.desc())
        .first())
    if not latest_msg or not latest_msg.thread_id:
        return jsonify({"messages": []})

    msgs = (EmailMessage.query
        .filter_by(thread_id=latest_msg.thread_id)
        .filter(EmailMessage.campaign_id.in_(account_cids))
        .order_by(EmailMessage.timestamp_email.asc()).all())

    out = []
    for m in msgs:
        body = m.body_text or ""
        lines = [l.strip() for l in body.splitlines() if l.strip()]
        clean = "\n".join(lines)[:1200]
        out.append({
            "inbound": m.ue_type == 2,
            "from_address": m.from_address or m.eaccount or "",
            "date": m.timestamp_email.strftime("%-d %b %Y, %-I:%M %p") if m.timestamp_email else "",
            "body": clean,
        })
    return jsonify({"messages": out})


@app.route("/admin/accounts/<int:aid>/tokens/generate", methods=["POST"])
@superadmin_required
def admin_token_generate(aid):
    import secrets
    acct = InstantlyAccount.query.get_or_404(aid)
    label = request.form.get("label", "").strip() or acct.name
    exp_days = request.form.get("expires_days", "").strip()
    expires_at = None
    if exp_days:
        try:
            expires_at = datetime.utcnow() + timedelta(days=int(exp_days))
        except ValueError:
            pass
    token_str = secrets.token_urlsafe(32)
    ct = ClientToken(token=token_str, account_id=aid, label=label, expires_at=expires_at)
    db.session.add(ct)
    db.session.commit()
    flash(f"Client link generated for '{acct.name}'.", "success")
    return redirect(url_for("admin", tab="tokens"))


@app.route("/admin/tokens/<int:tid>/revoke", methods=["POST"])
@superadmin_required
def admin_token_revoke(tid):
    ct = ClientToken.query.get_or_404(tid)
    ct.is_active = False
    db.session.commit()
    flash("Client link revoked.", "success")
    return redirect(url_for("admin", tab="tokens"))


_start_bg = not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true"
if _start_bg:
    _interval = int(os.getenv("AUTO_SYNC_MINUTES", "60")) * 60
    threading.Thread(target=_auto_sync_loop, args=(_interval,), daemon=True).start()
    log.info(f"Auto-sync thread started (every {_interval // 60} min)")


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5050))
    print(f"\n  Outreach Analytics → http://localhost:{port}\n")
    app.run(debug=True, port=port)
