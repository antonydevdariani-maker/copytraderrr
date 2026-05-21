import argparse
import os
import time
import logging
import requests
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

LEADERBOARD_URL = "https://data-api.polymarket.com/v1/leaderboard?timePeriod=MONTH&orderBy=PNL&limit=50"
POSITIONS_URL = "https://data-api.polymarket.com/positions?user={wallet}&sizeThreshold=.1"
CONSENSUS_THRESHOLD = 15
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
    r = requests.get(LEADERBOARD_URL, timeout=15)
    r.raise_for_status()
    return [entry["proxyWallet"] for entry in r.json()]


def fetch_positions(wallet):
    try:
        r = requests.get(POSITIONS_URL.format(wallet=wallet), timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"  positions fetch failed for {wallet[:10]}…: {e}")
        return []


def send_telegram_alert(markets):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    lines = ["🔥 *Polymarket Consensus Alert*", ""]
    for m in markets:
        lines.append(f"*{m['title']}*")
        lines.append(f"{m['count']}/50 {m['outcome']}  |  price: ${m['price']:.2f}")
        lines.append(m["url"])
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

    log.info("Fetching leaderboard…")
    wallets = fetch_leaderboard()
    log.info(f"Got {len(wallets)} wallets")

    # (conditionId, outcome) → set of wallets
    position_wallets = defaultdict(set)
    # (conditionId, outcome) → market metadata
    market_info = {}

    log.info("Fetching positions…")
    for i, wallet in enumerate(wallets, 1):
        positions = fetch_positions(wallet)
        for p in positions:
            cid = p.get("conditionId")
            outcome = p.get("outcome", "").upper()
            price = p.get("curPrice", 0)
            if not cid or not outcome:
                continue
            key = (cid, outcome)
            position_wallets[key].add(wallet)
            if key not in market_info:
                market_info[key] = {
                    "title": p.get("title", "Unknown"),
                    "price": price,
                    "event_slug": p.get("eventSlug", ""),
                }
        if i % 10 == 0:
            log.info(f"  {i}/50 done")
        time.sleep(0.1)

    all_sorted = sorted(
        [(key, len(ws)) for key, ws in position_wallets.items()],
        key=lambda x: x[1],
        reverse=True,
    )

    # filter: active markets only (price > 0)
    active = [(key, count) for key, count in all_sorted if market_info[key]["price"] > 0]

    log.info(f"\n--- Top 5 active markets by consensus ---")
    for (cid, outcome), count in active[:5]:
        m = market_info[(cid, outcome)]
        log.info(f"  {count}/50  {outcome}  ${m['price']:.2f}  |  {m['title']}")

    consensus = [(key, count) for key, count in active if count >= threshold]

    log.info(f"\nConsensus markets (≥{threshold}/50 traders):\n")
    alerts = []
    if not consensus:
        log.info("  None found.")
    else:
        for (cid, outcome), count in consensus:
            m = market_info[(cid, outcome)]
            url = f"https://polymarket.com/event/{m['event_slug']}" if m["event_slug"] else "https://polymarket.com"
            log.info(f"  {m['title']}")
            log.info(f"    {count}/50 {outcome}  |  price: ${m['price']:.2f}")
            log.info(f"    {url}")
            log.info("")
            alerts.append({
                "title": m["title"],
                "count": count,
                "outcome": outcome,
                "price": m["price"],
                "url": url,
            })

    if alerts:
        send_telegram_alert(alerts)


if __name__ == "__main__":
    import schedule

    parser = argparse.ArgumentParser()
    parser.add_argument("--threshold", type=int, default=CONSENSUS_THRESHOLD)
    args = parser.parse_args()

    run_pipeline(threshold=args.threshold)
    schedule.every(30).minutes.do(run_pipeline, threshold=args.threshold)
    while True:
        schedule.run_pending()
        time.sleep(30)
