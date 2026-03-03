import time
import logging
import requests

# -------------------------------
# CONFIG
# -------------------------------
BASE_URL = "https://api.github.com"
TOKEN = "GITHUB_TOEKN"

if not TOKEN:
    raise RuntimeError("GITHUB_TOKEN environment variable not set")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json"
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# -------------------------------
# RATE LIMIT HANDLING
# -------------------------------
def handle_rate_limit(resp):
    remaining = int(resp.headers.get("X-RateLimit-Remaining", 1))
    reset_ts = int(resp.headers.get("X-RateLimit-Reset", 0))

    if remaining == 0:
        sleep_for = max(reset_ts - time.time(), 0)
        log.warning("Rate limit hit. Sleeping for %.2f seconds", sleep_for)
        time.sleep(sleep_for)

# -------------------------------
# SAFE GET WITH RETRIES
# -------------------------------
def safe_get(url, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)

            if resp.status_code == 200:
                handle_rate_limit(resp)
                return resp

            if resp.status_code in (429, 500, 502, 503):
                handle_rate_limit(resp)
                sleep = 2 ** attempt
                log.warning(
                    "Retryable error %s. Retry %d in %ds",
                    resp.status_code, attempt + 1, sleep
                )
                time.sleep(sleep)
                continue

            # Non-retryable
            log.error("Non-retryable error %s: %s", resp.status_code, resp.text)
            resp.raise_for_status()

        except requests.exceptions.Timeout:
            sleep = 2 ** attempt
            log.warning("Timeout. Retry %d in %ds", attempt + 1, sleep)
            time.sleep(sleep)

    raise RuntimeError("GitHub API failed after retries")

# -------------------------------
# PAGINATION HANDLER
# -------------------------------
def fetch_all_pages(url):
    results = []

    while url:
        resp = safe_get(url)
        data = resp.json()
        results.extend(data)

        log.info(
            "Fetched %d records (remaining rate limit: %s)",
            len(data),
            resp.headers.get("X-RateLimit-Remaining")
        )

        url = resp.links.get("next", {}).get("url")

    return results

# -------------------------------
# API PLAYGROUND FUNCTIONS
# -------------------------------
def get_authenticated_user():
    url = f"{BASE_URL}/user"
    resp = safe_get(url)
    return resp.json()

def get_user_repos(username):
    url = f"{BASE_URL}/users/{username}/repos?per_page=10"
    return fetch_all_pages(url)

def get_org_repos(org):
    url = f"{BASE_URL}/orgs/{org}/repos?per_page=10"
    return fetch_all_pages(url)

def check_rate_limit():
    url = f"{BASE_URL}/rate_limit"
    resp = safe_get(url)
    return resp.json()

# -------------------------------
# MAIN (PLAY HERE)
# -------------------------------
if __name__ == "__main__":
    # 1️⃣ Auth test
    user = get_authenticated_user()
    log.info("Authenticated as: %s", user["login"])

    # 2️⃣ Fetch repos (change to org if you want)
    repos = get_user_repos(user["login"])
    log.info("Total repos fetched: %d", len(repos))

    # 3️⃣ Print sample repo data
    for r in repos[:5]:
        print({
            "id": r["id"],
            "name": r["name"],
            "stars": r["stargazers_count"],
            "updated_at": r["updated_at"]
        })

    # 4️⃣ Check rate limit status
    rate = check_rate_limit()
    print("\nRate limit status:")
    print(rate["rate"])