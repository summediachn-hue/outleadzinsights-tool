import json
import logging
import uuid
from datetime import datetime, timezone

from calendly_client import CalendlyClient
from models import db, CalendlyAccount, Prospect, Event

log = logging.getLogger(__name__)


def _dt(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def run_calendly_sync(cal: CalendlyAccount) -> dict:
    summary = {"bookings": 0, "new_leads": 0, "updated": 0, "errors": []}
    client = CalendlyClient(cal.api_token)
    aid = cal.account_id

    try:
        # Pull events since last sync (or all time if first sync)
        min_time = None
        if cal.last_synced_at:
            min_time = cal.last_synced_at.strftime("%Y-%m-%dT%H:%M:%SZ")

        events = client.get_scheduled_events(cal.user_uri, min_start_time=min_time)
        summary["bookings"] = len(events)

        for ev in events:
            event_uuid = ev["uri"].split("/")[-1]
            event_type_name = ev.get("name", "")
            start_time = _dt(ev.get("start_time"))

            try:
                invitees = client.get_invitees(event_uuid)
            except Exception as e:
                log.warning(f"Could not fetch invitees for {event_uuid}: {e}")
                continue

            for inv in invitees:
                email = (inv.get("email") or "").lower().strip()
                if not email:
                    continue

                raw_name = inv.get("name") or ""
                name_parts = raw_name.split(" ", 1)
                first_name = name_parts[0] if name_parts else ""
                last_name = name_parts[1] if len(name_parts) > 1 else ""

                company, job_title = "", ""
                for qa in inv.get("questions_and_answers", []):
                    q = (qa.get("question") or "").lower()
                    a = qa.get("answer") or ""
                    if any(w in q for w in ("company", "organization", "employer")):
                        company = a
                    elif any(w in q for w in ("title", "role", "position", "job")):
                        job_title = a

                existing = Prospect.query.filter_by(email=email, account_id=aid).first()
                if existing:
                    if existing.stage not in ("Won", "Lost"):
                        existing.stage = "Meeting"
                        existing.stage_changed_at = datetime.utcnow()
                    existing.calendly_event_type = event_type_name
                    existing.calendly_scheduled_at = start_time
                    if company and not existing.company_name:
                        existing.company_name = company
                    if job_title and not existing.job_title:
                        existing.job_title = job_title
                    summary["updated"] += 1
                    pid = existing.id
                else:
                    pid = f"cal_{aid}_{uuid.uuid4().hex[:12]}"
                    p = Prospect(
                        id=pid, email=email,
                        first_name=first_name, last_name=last_name,
                        company_name=company, job_title=job_title,
                        source="calendly", stage="Meeting",
                        stage_changed_at=datetime.utcnow(),
                        calendly_event_type=event_type_name,
                        calendly_scheduled_at=start_time,
                        account_id=aid,
                    )
                    db.session.add(p)
                    summary["new_leads"] += 1

                try:
                    db_ev = Event(
                        prospect_id=pid, prospect_email=email,
                        type="meeting_booked",
                        occurred_at=start_time or datetime.utcnow(),
                        source="sync",
                        meta=json.dumps({"source": "calendly", "event_type": event_type_name}),
                        account_id=aid,
                    )
                    db.session.add(db_ev)
                    db.session.flush()
                except Exception:
                    db.session.rollback()

        cal.last_synced_at = datetime.utcnow()
        cal.booking_count = (cal.booking_count or 0) + summary["new_leads"]
        if summary["bookings"]:
            cal.last_booking_at = datetime.utcnow()
        db.session.commit()

    except Exception as e:
        log.error(f"Calendly sync error: {e}")
        summary["errors"].append(str(e))
        db.session.rollback()

    return summary
