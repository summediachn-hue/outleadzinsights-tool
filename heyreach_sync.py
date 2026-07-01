import json
import logging
from datetime import datetime

from heyreach_client import HeyReachClient
from models import db, HeyReachAccount, HeyReachCampaign, HeyReachLead

log = logging.getLogger(__name__)


def _dt(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def run_heyreach_sync(hr_account: HeyReachAccount) -> dict:
    summary = {"campaigns": 0, "leads": 0, "errors": []}
    client = HeyReachClient(hr_account.api_key)
    account_id = hr_account.account_id
    hr_id = hr_account.id

    try:
        # ── 1. Campaigns ──────────────────────────────────────────────────────
        campaigns_raw = client.get_all_campaigns()
        list_to_campaign = {}   # list_id → campaign_id (first campaign wins)

        for cr in campaigns_raw:
            cid = cr["id"]
            ps = cr.get("progressStats", {})
            list_id = cr.get("linkedInUserListId")

            c = db.session.get(HeyReachCampaign, cid)
            if not c:
                c = HeyReachCampaign(id=cid, account_id=account_id, heyreach_account_id=hr_id)
                db.session.add(c)

            c.name = cr.get("name", "")
            c.status = cr.get("status", "")
            c.list_id = list_id
            c.list_name = cr.get("linkedInUserListName", "")
            c.total_leads = ps.get("totalUsers", 0)
            c.leads_in_progress = ps.get("totalUsersInProgress", 0)
            c.leads_finished = ps.get("totalUsersFinished", 0)
            c.leads_failed = ps.get("totalUsersFailed", 0)
            c.started_at = _dt(cr.get("startedAt"))
            c.created_at = _dt(cr.get("creationTime"))
            c.synced_at = datetime.utcnow()

            if list_id and list_id not in list_to_campaign:
                list_to_campaign[list_id] = cid

        db.session.commit()
        summary["campaigns"] = len(campaigns_raw)

        # ── 2. Lists + Leads ──────────────────────────────────────────────────
        lists_raw = client.get_all_lists()

        for lst in lists_raw:
            list_id = lst["id"]
            campaign_id = list_to_campaign.get(list_id)
            leads_raw = client.get_leads_from_list(list_id)

            for lr in leads_raw:
                lid = str(lr["id"])
                lead = db.session.get(HeyReachLead, lid)
                if not lead:
                    lead = HeyReachLead(id=lid, account_id=account_id, heyreach_account_id=hr_id)
                    db.session.add(lead)

                lead.linkedin_id = str(lr.get("linkedin_id") or "")
                lead.linkedin_url = lr.get("profileUrl") or ""
                lead.first_name = lr.get("firstName") or ""
                lead.last_name = lr.get("lastName") or ""
                lead.headline = lr.get("headline") or ""
                lead.position = lr.get("position") or ""
                lead.company_name = lr.get("companyName") or ""
                lead.location = lr.get("location") or ""
                lead.email = lr.get("emailAddress") or lr.get("enrichedEmailAddress") or lr.get("customEmailAddress")
                lead.campaign_id = campaign_id
                lead.list_id = list_id

                # Resolve autoTags for this campaign (or any campaign)
                auto_tags = lr.get("autoTags", [])
                tag_names = {t["name"].lower() for t in auto_tags}
                lead.tag_interested    = "interested" in tag_names
                lead.tag_not_interested = "not interested" in tag_names
                lead.tag_generic       = "generic" in tag_names
                lead.raw_tags          = json.dumps(list(tag_names))

                # Auto-advance li_stage from HeyReach tags.
                # Never downgrade. Never touch Meeting / Won (user-confirmed stages).
                _LI_RANK = {"Contacted": 1, "Replied": 2, "Interested": 3,
                            "Meeting": 4, "Won": 5}
                cur_rank = _LI_RANK.get(lead.li_stage, 0)
                protected = lead.li_stage in ("Meeting", "Won")

                if lead.tag_interested and not protected:
                    if _LI_RANK["Interested"] > cur_rank:
                        lead.li_stage = "Interested"
                        lead.li_stage_changed_at = datetime.utcnow()
                elif lead.tag_generic and not protected:
                    if _LI_RANK["Replied"] > cur_rank:
                        lead.li_stage = "Replied"
                        lead.li_stage_changed_at = datetime.utcnow()
                elif lead.tag_not_interested and lead.li_stage in (None, "Contacted"):
                    # Auto-close only if still at the earliest stage
                    lead.li_stage = "Closed"
                    lead.li_stage_changed_at = datetime.utcnow()
                elif not lead.li_stage:
                    # Default: every synced lead starts as Contacted
                    lead.li_stage = "Contacted"

                lead.synced_at = datetime.utcnow()

            db.session.commit()
            summary["leads"] += len(leads_raw)

        # ── 3. Update account sync time ───────────────────────────────────────
        hr_account.synced_at = datetime.utcnow()
        db.session.commit()

    except Exception as e:
        log.error(f"HeyReach sync error: {e}")
        summary["errors"].append(str(e))
        db.session.rollback()

    return summary
