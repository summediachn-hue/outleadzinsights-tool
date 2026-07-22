"""
Instantly API V2 client: built against the REAL schema confirmed via live
introspection (see introspect.py / introspect2.py), not just the docs.

Auth:  Authorization: Bearer <api_key>
Base:  https://api.instantly.ai/api/v2
"""

import logging
import requests
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

BASE = "https://api.instantly.ai/api/v2"


# ── Enum decoders (confirmed from live data) ───────────────────────────────────

CAMPAIGN_STATUS = {
    0: "Draft", 1: "Active", 2: "Paused", 3: "Completed",
    4: "Running Subsequences", -99: "Suspended", -1: "Error",
}

# Lead sequence status (the `status` int on a lead)
LEAD_STATUS = {
    1: "Active", 2: "Paused", 3: "Completed", -1: "Stopped",
    -2: "Bounced", -3: "Unsubscribed",
}

# Lead interest/disposition (`lt_interest_status`): maps to our CRM dispositions
LEAD_INTEREST = {
    1: "Interested", 2: "Meeting Booked", 3: "Meeting Completed",
    4: "Closed", 0: "Out of Office", -1: "Not Interested",
    -2: "Wrong Person", -3: "Lost",
}

# Maps Instantly disposition → our CRM recycle behaviour
INTEREST_TO_DISPOSITION = {
    "Interested":        ("interested",     None),
    "Meeting Booked":    ("interested",     None),
    "Meeting Completed": ("interested",     None),
    "Closed":            ("interested",     None),
    "Out of Office":     ("not_now",        14),    # park 14 days
    "Not Interested":    ("not_interested", 90),    # recycle in 90 days
    "Wrong Person":      ("wrong_person",   14),    # re-route
    "Lost":              ("not_interested", 120),
}


class InstantlyError(Exception):
    pass


class InstantlyClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        })

    # ── Low-level ─────────────────────────────────────────────────────────────

    def _request(self, method: str, path: str, params: Dict = None, body: Dict = None) -> Any:
        url = f"{BASE}{path}"
        try:
            r = self.session.request(method, url, params=params, json=body, timeout=40)
        except requests.RequestException as e:
            raise InstantlyError(f"Network error: {e}")

        if r.status_code == 401:
            raise InstantlyError("Invalid API key: check INSTANTLY_API_KEY in .env")
        if r.status_code == 403:
            raise InstantlyError(f"Plan does not include this endpoint ({path})")
        if r.status_code == 404:
            raise InstantlyError(f"Endpoint not found ({path})")
        if r.status_code == 429:
            raise InstantlyError("Rate limited: wait a moment and retry")
        if not r.ok:
            raise InstantlyError(f"API error {r.status_code}: {r.text[:200]}")
        try:
            return r.json()
        except Exception:
            raise InstantlyError(f"Non-JSON response: {r.text[:200]}")

    def _get(self, path: str, params: Dict = None) -> Any:
        return self._request("GET", path, params=params)

    def _post(self, path: str, body: Dict = None, params: Dict = None) -> Any:
        return self._request("POST", path, params=params, body=body or {})

    @staticmethod
    def _items(result: Any) -> List[Dict]:
        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            for k in ("items", "data", "result"):
                if isinstance(result.get(k), list):
                    return result[k]
        return []

    def _paginate_get(self, path: str, params: Dict) -> List[Dict]:
        out: List[Dict] = []
        starting_after = None
        while True:
            p = {**params, "limit": 100}
            if starting_after:
                p["starting_after"] = starting_after
            result = self._get(path, p)
            items = self._items(result)
            if not items:
                break
            out.extend(items)
            starting_after = result.get("next_starting_after") if isinstance(result, dict) else None
            if not starting_after:
                break
        return out

    def _paginate_post(self, path: str, body: Dict) -> List[Dict]:
        out: List[Dict] = []
        starting_after = None
        while True:
            b = {**body, "limit": 100}
            if starting_after:
                b["starting_after"] = starting_after
            result = self._post(path, b)
            items = self._items(result)
            if not items:
                break
            out.extend(items)
            starting_after = result.get("next_starting_after") if isinstance(result, dict) else None
            if not starting_after:
                break
        return out

    # ── Connection test ───────────────────────────────────────────────────────

    def test_connection(self) -> bool:
        try:
            self._get("/campaigns", {"limit": 1})
            return True
        except InstantlyError:
            return False

    def workspace_current(self) -> Dict:
        """The connected workspace/account (has a human-readable `name`)."""
        try:
            r = self._get("/workspaces/current")
            return r if isinstance(r, dict) else {}
        except InstantlyError:
            return {}

    # ── Campaigns ─────────────────────────────────────────────────────────────

    def list_campaigns(self) -> List[Dict]:
        # Instantly v2 defaults to active only; fetch all statuses so paused/
        # completed campaigns get their status updated in the local DB too.
        seen: Dict[str, Dict] = {}
        for status_code in (1, 2, 3, 0, -1):  # Active, Paused, Completed, Draft, Error
            try:
                for c in self._paginate_get("/campaigns", {"status": status_code}):
                    cid = c.get("id")
                    if cid:
                        seen[cid] = c
            except Exception:
                pass
        # Fallback: unfiltered call catches anything the status loop missed
        for c in self._paginate_get("/campaigns", {}):
            cid = c.get("id")
            if cid:
                seen[cid] = c
        return list(seen.values())

    def campaign_overview(self, campaign_id: Optional[str] = None) -> Dict:
        """Aggregate analytics. With id → that campaign; without → all combined."""
        params = {"id": campaign_id} if campaign_id else {}
        result = self._get("/campaigns/analytics/overview", params)
        if isinstance(result, list):
            return result[0] if result else {}
        return result if isinstance(result, dict) else {}

    def campaign_daily(self, campaign_id: Optional[str] = None,
                       start_date: str = None, end_date: str = None) -> List[Dict]:
        """Per-day analytics → trends, heatmaps, period comparison."""
        params = {}
        if campaign_id:
            params["campaign_id"] = campaign_id
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._items(self._get("/campaigns/analytics/daily", params))

    def campaign_steps(self, campaign_id: str) -> List[Dict]:
        """Per-step + variant analytics → script analysis & A/B. Filters null steps.

        NOTE: this endpoint requires `campaign_id` (NOT `id` like overview does —
        passing `id` is silently ignored and returns workspace-global data).
        """
        rows = self._items(self._get("/campaigns/analytics/steps", {"campaign_id": campaign_id}))
        cleaned = []
        for s in rows:
            step = s.get("step")
            if step in (None, "null", ""):
                continue
            cleaned.append(s)
        return cleaned

    # ── Leads ─────────────────────────────────────────────────────────────────

    def list_leads(self, campaign_id: Optional[str] = None) -> List[Dict]:
        """POST /leads/list: confirmed endpoint (not GET)."""
        body = {}
        if campaign_id:
            body["campaign"] = campaign_id
        return self._paginate_post("/leads/list", body)

    # ── Emails ────────────────────────────────────────────────────────────────

    def list_emails_page(self, starting_after: Optional[str] = None,
                         limit: int = 100) -> tuple:
        """Fetch one page of emails newest-first. Returns (items, next_starting_after)."""
        params: Dict = {"limit": limit}
        if starting_after:
            params["starting_after"] = starting_after
        r = self._get("/emails", params)
        return r.get("items", []), r.get("next_starting_after")

    def send_reply(self, reply_to_id: str, eaccount: str, body: str,
                   subject: str = "") -> Dict:
        """Reply to an email thread via Instantly /emails/reply."""
        return self._post("/emails/reply", {
            "reply_to_uuid": reply_to_id,
            "eaccount": eaccount,
            "subject": subject,
            "body": {"html": body},
        })

    # ── Sending accounts + deliverability ─────────────────────────────────────

    def list_accounts(self) -> List[Dict]:
        return self._paginate_get("/accounts", {})

    def warmup_analytics(self, emails: List[str]) -> Dict:
        """
        Returns {aggregate_data: {email: {sent, received, landed_inbox,
        landed_spam, health_score}}, email_date_data: {email: {date: {...}}}}.
        """
        if not emails:
            return {}
        try:
            return self._post("/accounts/warmup-analytics", {"emails": emails})
        except InstantlyError as e:
            log.warning("warmup analytics unavailable: %s", e)
            return {}

    # ── Webhooks (live event store feeder) ────────────────────────────────────

    def list_webhooks(self) -> List[Dict]:
        try:
            return self._items(self._get("/webhooks", {"limit": 100}))
        except InstantlyError:
            return []

    def create_webhook(self, url: str, event_types: List[str]) -> Dict:
        return self._post("/webhooks", {"url": url, "event_types": event_types})

    # ── Normalisation helpers ─────────────────────────────────────────────────

    @staticmethod
    def parse_int(*candidates) -> int:
        for v in candidates:
            if isinstance(v, bool):
                continue
            if isinstance(v, (int, float)):
                return int(v)
        return 0

    @staticmethod
    def campaign_status_label(code) -> str:
        return CAMPAIGN_STATUS.get(code, f"Unknown ({code})")

    @staticmethod
    def lead_status_label(code) -> str:
        return LEAD_STATUS.get(code, f"Unknown ({code})")

    @staticmethod
    def interest_label(code) -> Optional[str]:
        if code in (None, ""):
            return None
        return LEAD_INTEREST.get(code, f"Custom ({code})")
