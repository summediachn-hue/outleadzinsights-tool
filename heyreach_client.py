import requests

HEYREACH_BASE = "https://api.heyreach.io/api/public"


class HeyReachError(Exception):
    pass


class HeyReachClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "X-API-KEY": api_key,
            "Content-Type": "application/json",
        })

    def _post(self, endpoint: str, payload: dict = None) -> dict:
        resp = self.session.post(f"{HEYREACH_BASE}/{endpoint}", json=payload or {}, timeout=30)
        if resp.status_code == 401:
            raise HeyReachError("Invalid HeyReach API key")
        if not resp.ok:
            raise HeyReachError(f"HeyReach API error {resp.status_code}: {resp.text[:200]}")
        return resp.json()

    def check_key(self) -> bool:
        resp = self.session.get(f"{HEYREACH_BASE}/auth/CheckApiKey", timeout=10)
        return resp.status_code == 200

    def get_all_campaigns(self) -> list:
        results = []
        offset = 0
        limit = 50
        while True:
            data = self._post("Campaign/GetAll", {"offset": offset, "limit": limit})
            items = data.get("items", [])
            results.extend(items)
            if len(results) >= data.get("totalCount", 0) or not items:
                break
            offset += limit
        return results

    def get_all_lists(self) -> list:
        results = []
        offset = 0
        limit = 50
        while True:
            data = self._post("List/GetAll", {"offset": offset, "limit": limit})
            items = data.get("items", [])
            results.extend(items)
            if len(results) >= data.get("totalCount", 0) or not items:
                break
            offset += limit
        return results

    def get_leads_from_list(self, list_id: int) -> list:
        results = []
        offset = 0
        limit = 100
        while True:
            data = self._post("List/GetLeadsFromList",
                              {"listId": list_id, "offset": offset, "limit": limit})
            items = data.get("items", [])
            results.extend(items)
            if len(results) >= data.get("totalCount", 0) or not items:
                break
            offset += limit
        return results
