import requests

CALENDLY_BASE = "https://api.calendly.com"


class CalendlyError(Exception):
    pass


class CalendlyClient:
    def __init__(self, api_token: str):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        })

    def _get(self, endpoint: str, params: dict = None) -> dict:
        resp = self.session.get(f"{CALENDLY_BASE}/{endpoint}", params=params, timeout=30)
        if resp.status_code == 401:
            raise CalendlyError("Invalid Calendly personal access token")
        if not resp.ok:
            raise CalendlyError(f"Calendly API error {resp.status_code}: {resp.text[:200]}")
        return resp.json()

    def get_current_user(self) -> dict:
        return self._get("users/me")["resource"]

    def get_scheduled_events(self, user_uri: str, min_start_time: str = None,
                             max_start_time: str = None) -> list:
        results = []
        params = {"user": user_uri, "count": 100, "status": "active"}
        if min_start_time:
            params["min_start_time"] = min_start_time
        if max_start_time:
            params["max_start_time"] = max_start_time
        while True:
            data = self._get("scheduled_events", params)
            results.extend(data.get("collection", []))
            next_page = data.get("pagination", {}).get("next_page_token")
            if not next_page:
                break
            params["page_token"] = next_page
        return results

    def get_invitees(self, event_uuid: str) -> list:
        data = self._get(f"scheduled_events/{event_uuid}/invitees", {"count": 100})
        return data.get("collection", [])
