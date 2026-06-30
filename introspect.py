"""
Instantly API V2 — live introspection tool.

Probes every analytics-relevant endpoint against YOUR account and reports:
  - which endpoints are accessible on your plan
  - the exact field names each returns (the real schema, not the docs)
  - a PII-masked sample value so we can see data types
  - a summary of which analytics modules are buildable

Run:  python3 introspect.py
Safe to share the output — emails / names / phones are masked.
"""

import json
import os
import sys
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")  # silence LibreSSL/urllib3 notice on system python

import requests
from dotenv import load_dotenv

load_dotenv()

BASE = "https://api.instantly.ai/api/v2"
KEY = os.getenv("INSTANTLY_API_KEY", "").strip()

# Fields whose VALUES we mask (we always still show the field NAME)
PII_KEYS = {"email", "first_name", "last_name", "phone", "name", "to_address_email_list",
            "company_name", "lead", "from_address_email", "to", "personalization"}

# ── pretty printing ────────────────────────────────────────────────────────────

class C:
    G = "\033[92m"; R = "\033[91m"; Y = "\033[93m"; B = "\033[94m"
    DIM = "\033[2m"; BOLD = "\033[1m"; END = "\033[0m"

def head(t):
    print(f"\n{C.BOLD}{C.B}{'─'*70}{C.END}")
    print(f"{C.BOLD}{C.B}  {t}{C.END}")
    print(f"{C.BOLD}{C.B}{'─'*70}{C.END}")

def ok(t):   print(f"  {C.G}✓{C.END} {t}")
def bad(t):  print(f"  {C.R}✗{C.END} {t}")
def warn(t): print(f"  {C.Y}!{C.END} {t}")
def dim(t):  print(f"  {C.DIM}{t}{C.END}")


def mask(key, val):
    """Mask PII values but preserve type info."""
    kl = str(key).lower()
    if any(p in kl for p in PII_KEYS):
        if isinstance(val, str) and val:
            return f"<{kl}:masked>"
        if isinstance(val, list):
            return f"<list[{len(val)}]:masked>"
    if isinstance(val, str) and len(val) > 60:
        return val[:57] + "…"
    return val


def describe_item(item, indent="    "):
    """Print field names + masked sample values of a dict."""
    if not isinstance(item, dict):
        dim(f"{indent}(non-dict item: {type(item).__name__} = {mask('x', item)})")
        return []
    fields = sorted(item.keys())
    for k in fields:
        v = item[k]
        tname = type(v).__name__
        sample = mask(k, v)
        if isinstance(v, dict):
            print(f"{indent}{C.BOLD}{k}{C.END} {C.DIM}(object: {', '.join(list(v.keys())[:8])}){C.END}")
        elif isinstance(v, list):
            inner = f"[{len(v)}]"
            if v and isinstance(v[0], dict):
                inner += f" of objects ({', '.join(list(v[0].keys())[:5])})"
            print(f"{indent}{C.BOLD}{k}{C.END} {C.DIM}(list{inner}){C.END}")
        else:
            print(f"{indent}{C.BOLD}{k}{C.END} {C.DIM}({tname}){C.END} = {sample}")
    return fields


def call(method, path, params=None, body=None):
    url = f"{BASE}{path}"
    headers = {"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}
    try:
        if method == "GET":
            r = requests.get(url, headers=headers, params=params, timeout=30)
        else:
            r = requests.post(url, headers=headers, params=params, json=body or {}, timeout=30)
    except requests.RequestException as e:
        return None, f"network error: {e}"
    return r, None


def extract_list(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("items", "data", "campaigns", "leads", "accounts", "emails", "result"):
            if isinstance(data.get(key), list):
                return data[key]
    return None


# ── probe runner ────────────────────────────────────────────────────────────────

results = {}  # name -> bool accessible

def probe(name, method, path, params=None, body=None, note=""):
    head(f"{name}   {C.DIM}{method} {path}{C.END}")
    if note:
        dim(note)
    r, err = call(method, path, params, body)
    if err:
        bad(err); results[name] = False; return None
    if r.status_code == 401:
        bad("401 Unauthorized — API key invalid or missing"); results[name] = False; return None
    if r.status_code == 403:
        warn("403 Forbidden — endpoint exists but your PLAN doesn't include it"); results[name] = False; return None
    if r.status_code == 404:
        warn("404 Not Found — endpoint path may differ on your version"); results[name] = False; return None
    if not r.ok:
        bad(f"HTTP {r.status_code}: {r.text[:160]}"); results[name] = False; return None

    try:
        data = r.json()
    except Exception:
        bad(f"non-JSON response: {r.text[:120]}"); results[name] = False; return None

    ok(f"HTTP {r.status_code} — accessible")
    results[name] = True

    lst = extract_list(data)
    if lst is not None:
        print(f"  {C.DIM}→ returned a list of {len(lst)} item(s){C.END}")
        if lst:
            print(f"  {C.BOLD}Fields per item:{C.END}")
            describe_item(lst[0])
        else:
            warn("list is empty — no records of this type in your account yet")
        return data
    if isinstance(data, dict):
        print(f"  {C.BOLD}Fields:{C.END}")
        describe_item(data)
    return data


def main():
    head("INSTANTLY API V2 — LIVE INTROSPECTION")
    if not KEY:
        bad("No INSTANTLY_API_KEY found in .env")
        print(f"\n  Add your key to {C.BOLD}.env{C.END}:")
        print(f"  {C.DIM}INSTANTLY_API_KEY=your_key_here{C.END}\n")
        sys.exit(1)
    print(f"  Key detected: {C.G}{KEY[:6]}…{KEY[-4:]}{C.END}")
    print(f"  Time: {datetime.now():%Y-%m-%d %H:%M}")

    today = datetime.now().date()
    month_ago = today - timedelta(days=30)

    # 1. Campaigns — also grab an id for dependent probes
    camp_data = probe("Campaigns", "GET", "/campaigns", params={"limit": 5})
    campaign_id = None
    camp_list = extract_list(camp_data) if camp_data else None
    if camp_list:
        campaign_id = camp_list[0].get("id") or camp_list[0].get("campaign_id")
        dim(f"using campaign id for dependent probes: {campaign_id}")

    # 2. Campaign analytics — overview (all campaigns)
    probe("Campaign Analytics · Overview", "GET", "/campaigns/analytics/overview")

    # 3. Daily campaign analytics → trends/heatmaps
    probe("Campaign Analytics · Daily", "GET", "/campaigns/analytics/daily",
          params={"start_date": str(month_ago), "end_date": str(today)},
          note="Per-day metrics → powers trends, heatmaps, period comparison")

    # 4. Steps analytics → script + A/B
    if campaign_id:
        probe("Campaign Analytics · Steps", "GET", "/campaigns/analytics/steps",
              params={"id": campaign_id},
              note="Per step + variant → script analysis & A/B winner detection")
    else:
        warn("skipping Steps analytics — no campaign id available")

    # 5. Leads — V2 uses POST /leads/list
    probe("Leads (list)", "POST", "/leads/list", body={"limit": 3},
          note="Per-lead engagement: open/reply/click counts, last_open ts, interest status, ai_interest_value")

    # 6. Emails — actual messages incl. replies + AI sentiment
    probe("Emails", "GET", "/emails", params={"limit": 3},
          note="Actual sent/received emails → reply content, ai_interest_value, reply timing")

    # 7. Sending accounts — health
    acct_data = probe("Sending Accounts", "GET", "/accounts", params={"limit": 5},
                      note="Mailbox health: warmup_score, daily_limit, status, errors")
    acct_email = None
    acct_list = extract_list(acct_data) if acct_data else None
    if acct_list:
        acct_email = acct_list[0].get("email")

    # 8. Warmup analytics — inbox vs spam placement
    probe("Warmup Analytics (deliverability)", "POST", "/accounts/warmup-analytics",
          body={"emails": [acct_email]} if acct_email else {},
          note="landed_inbox vs landed_spam + health_score → THE deliverability layer")

    # 9. Daily account analytics
    probe("Daily Account Analytics", "GET", "/accounts/daily-analytics",
          params={"start_date": str(month_ago), "end_date": str(today)},
          note="Per-mailbox per-day sent + bounced")

    # 10. Webhooks — event stream config
    probe("Webhooks", "GET", "/webhooks", params={"limit": 5},
          note="Real-time event subscriptions → the live event store feeder")

    # ── Summary ──────────────────────────────────────────────────────────────────
    head("SUMMARY — what your account can power")
    modules = {
        "Engagement + Funnel":      ["Campaign Analytics · Overview", "Leads (list)"],
        "Trends + Send-time heatmap":["Campaign Analytics · Daily"],
        "Script + A/B analysis":    ["Campaign Analytics · Steps"],
        "Reply sentiment":          ["Emails"],
        "Deliverability / inbox":   ["Warmup Analytics (deliverability)", "Sending Accounts"],
        "Revenue / conversion":     ["Campaign Analytics · Overview"],
        "Live event store":         ["Webhooks"],
    }
    for mod, deps in modules.items():
        got = all(results.get(d) for d in deps)
        some = any(results.get(d) for d in deps)
        if got:
            ok(f"{mod}")
        elif some:
            warn(f"{mod}  (partial — some endpoints gated/empty)")
        else:
            bad(f"{mod}  (endpoints not accessible)")

    print(f"\n  {C.DIM}Accessible endpoints: "
          f"{sum(1 for v in results.values() if v)}/{len(results)}{C.END}\n")


if __name__ == "__main__":
    main()
