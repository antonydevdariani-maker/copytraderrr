import argparse
import os
import time
import logging
import requests
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

LEADERBOARD_URL = "https://data-api.polymarket.com/v1/leaderboard?timePeriod=MONTH&orderBy=PNL&limit=50&offset={offset}"
POSITIONS_URL = "https://data-api.polymarket.com/positions?user={wallet}&sizeThreshold=.1"
GAMMA_URL = "https://gamma-api.polymarket.com/markets?conditionId={cid}"
ACTIVITY_URL = "https://data-api.polymarket.com/activity?user={wallet}&limit=500"
CONSENSUS_THRESHOLD = 5
LEADERBOARD_TOTAL = 200
EXPIRY_DAYS = 30

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[
        logging.FileHandler("consensus_log.txt"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def fetch_leaderboard():
    wallets = []
    offset = 0
    while len(wallets) < LEADERBOARD_TOTAL:
        r = requests.get(LEADERBOARD_URL.format(offset=offset), timeout=15)
        r.raise_for_status()
        page = r.json()
        if not page:
            break
        wallets.extend(entry["proxyWallet"] for entry in page)
        offset += 50
    return wallets[:LEADERBOARD_TOTAL]


def fetch_positions(wallet):
    try:
        r = requests.get(POSITIONS_URL.format(wallet=wallet), timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"  positions fetch failed for {wallet[:10]}…: {e}")
        return []


def fetch_activity(wallet):
    try:
        r = requests.get(ACTIVITY_URL.format(wallet=wallet), timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"  activity fetch failed for {wallet[:10]}…: {e}")
        return []


def is_active_market(cid):
    try:
        r = requests.get(GAMMA_URL.format(cid=cid), timeout=15)
        r.raise_for_status()
        data = r.json()
        if not data:
            return False
        m = data[0]
        return m.get("active", False) and not m.get("closed", True)
    except Exception as e:
        log.warning(f"  gamma check failed for {cid[:10]}…: {e}")
        return False


def within_expiry(end_date_str):
    if not end_date_str:
        return False
    try:
        if "T" in end_date_str:
            end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        else:
            end_dt = datetime.fromisoformat(end_date_str).replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return timedelta(0) < (end_dt - now) <= timedelta(days=EXPIRY_DAYS)
    except Exception:
        return False


def send_telegram_alert(markets, total_wallets):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    lines = ["🔥 *Polymarket Consensus Alert*", ""]
    for m in markets[:10]:
        lines.append(f"*{m['title']}*")
        lines.append(f"{m['count']}/{total_wallets} {m['outcome']}  |  price: ${m['price']:.2f}")
        lines.append(f"Ends: {m['end_date']}")
        if m.get("earliest_entry"):
            from datetime import datetime, timezone
            earliest = datetime.fromtimestamp(m["earliest_entry"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            latest = datetime.fromtimestamp(m["latest_entry"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            if earliest == latest:
                lines.append(f"First trade: {earliest}")
            else:
                lines.append(f"First trade: {earliest}")
                lines.append(f"Last trade: {latest}")
        lines.append(m["url"])
        trader_links = "  ".join(
            f"[trader {i+1}](https://polymarket.com/profile/{w})"
            for i, w in enumerate(m.get("wallets", [])[:5])
        )
        if trader_links:
            lines.append(trader_links)
        lines.append("")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": "\n".join(lines),
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        log.info("Telegram alert sent.")
    except Exception as e:
        log.warning(f"Telegram alert failed: {e}")


def run_pipeline(threshold=CONSENSUS_THRESHOLD):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log.info(f"\n{'='*60}")
    log.info(f"=== {ts} ===")
    log.info(f"{'='*60}")

    log.info(f"Fetching top {LEADERBOARD_TOTAL} wallets…")
    wallets = fetch_leaderboard()
    total = len(wallets)
    log.info(f"Got {total} wallets")

    position_wallets = defaultdict(set)
    market_info = {}
    trade_timestamps = defaultdict(dict)  # (cid, outcome) -> {wallet: earliest_buy_ts}

    log.info("Fetching positions…")
    for i, wallet in enumerate(wallets, 1):
        positions = fetch_positions(wallet)
        for p in positions:
            cid = p.get("conditionId")
            outcome = p.get("outcome", "").upper()
            price = p.get("curPrice", 0)
            end_date = p.get("endDate", "")
            if not cid or not outcome or not price:
                continue
            if not within_expiry(end_date):
                continue
            key = (cid, outcome)
            position_wallets[key].add(wallet)
            if key not in market_info:
                market_info[key] = {
                    "title": p.get("title", "Unknown"),
                    "price": price,
                    "event_slug": p.get("eventSlug", ""),
                    "end_date": end_date[:10],
                    "cid": cid,
                }
        # fetch activity and record earliest BUY per market
        activity = fetch_activity(wallet)
        for a in activity:
            if a.get("type") != "TRADE" or a.get("side") != "BUY":
                continue
            cid = a.get("conditionId")
            outcome = a.get("outcome", "").upper()
            ts_val = a.get("timestamp")
            if not cid or not outcome or not ts_val:
                continue
            key = (cid, outcome)
            if wallet in trade_timestamps[key]:
                trade_timestamps[key][wallet] = min(trade_timestamps[key][wallet], ts_val)
            else:
                trade_timestamps[key][wallet] = ts_val
        if i % 50 == 0:
            log.info(f"  {i}/{total} done")
        time.sleep(0.1)

    all_sorted = sorted(
        [(key, len(ws)) for key, ws in position_wallets.items()],
        key=lambda x: x[1],
        reverse=True,
    )

    candidates = [(key, count) for key, count in all_sorted if count >= max(threshold // 2, 2)]
    log.info(f"\nValidating {len(candidates)} candidates against gamma API…")
    validated = []
    checked_cids = {}

    for key, count in candidates:
        cid = market_info[key]["cid"]
        if cid not in checked_cids:
            checked_cids[cid] = is_active_market(cid)
            time.sleep(0.05)
        if checked_cids[cid]:
            validated.append((key, count))

    log.info(f"\n--- Top 5 active markets ending <=30 days ---")
    if not validated:
        log.info("  None found.")
    for (cid, outcome), count in validated[:5]:
        m = market_info[(cid, outcome)]
        log.info(f"  {count}/{total}  {outcome}  ${m['price']:.2f}  ends {m['end_date']}  |  {m['title']}")

    consensus = validated  # alert on all active markets with any shared positions

    log.info(f"\nConsensus markets (>={threshold}/{total} traders):\n")
    alerts = []
    if not consensus:
        log.info("  None found.")
    else:
        for (cid, outcome), count in consensus:
            m = market_info[(cid, outcome)]
            url = f"https://polymarket.com/event/{m['event_slug']}" if m["event_slug"] else "https://polymarket.com"
            log.info(f"  {m['title']}")
            log.info(f"    {count}/{total} {outcome}  |  price: ${m['price']:.2f}  |  ends {m['end_date']}")
            log.info(f"    {url}")
            log.info("")
            ts_map = trade_timestamps.get((cid, outcome), {})
            earliest = min(ts_map.values()) if ts_map else None
            latest = max(ts_map.values()) if ts_map else None
            alerts.append({
                "title": m["title"],
                "count": count,
                "outcome": outcome,
                "price": m["price"],
                "end_date": m["end_date"],
                "url": url,
                "wallets": list(position_wallets[(cid, outcome)]),
                "earliest_entry": earliest,
                "latest_entry": latest,
            })

    if alerts:
        send_telegram_alert(alerts, total)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--threshold", type=int, default=CONSENSUS_THRESHOLD)
    args = parser.parse_args()

    # wait for network on boot (launchd starts before DNS is ready)
    for attempt in range(10):
        try:
            requests.get("https://data-api.polymarket.com", timeout=5)
            break
        except Exception:
            log.info(f"Waiting for network… attempt {attempt + 1}/10")
            time.sleep(15)

    run_pipeline(threshold=args.threshold)
