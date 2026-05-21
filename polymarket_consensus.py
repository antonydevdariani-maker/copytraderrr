import argparse
import time
import logging
import requests
from collections import defaultdict
from datetime import datetime

LEADERBOARD_URL = "https://data-api.polymarket.com/v1/leaderboard?timePeriod=MONTH&orderBy=PNL&limit=50"
POSITIONS_URL = "https://data-api.polymarket.com/positions?user={wallet}&sizeThreshold=.1"
CONSENSUS_THRESHOLD = 15

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
    # (conditionId, outcome) → (title, curPrice)
    market_info = {}

    log.info("Fetching positions…")
    for i, wallet in enumerate(wallets, 1):
        positions = fetch_positions(wallet)
        for p in positions:
            cid = p.get("conditionId")
            outcome = p.get("outcome", "").upper()
            if not cid or not outcome:
                continue
            key = (cid, outcome)
            position_wallets[key].add(wallet)
            if key not in market_info:
                market_info[key] = (p.get("title", "Unknown"), p.get("curPrice", 0))
        if i % 10 == 0:
            log.info(f"  {i}/50 done")
        time.sleep(0.1)

    all_sorted = sorted(
        [(key, len(ws)) for key, ws in position_wallets.items()],
        key=lambda x: x[1],
        reverse=True,
    )

    log.info(f"\n--- Top 5 markets by consensus (all traders) ---")
    for (cid, outcome), count in all_sorted[:5]:
        title, price = market_info.get((cid, outcome), ("Unknown", 0))
        price_str = f"${price:.2f}" if price else "N/A"
        log.info(f"  {count}/50  {outcome}  {price_str}  |  {title}")

    consensus = [(key, count) for key, count in all_sorted if count >= threshold]

    log.info(f"\nConsensus markets (≥{threshold}/50 traders):\n")
    if not consensus:
        log.info("  None found.")
    else:
        for (cid, outcome), count in consensus:
            title, price = market_info.get((cid, outcome), ("Unknown", 0))
            price_str = f"${price:.2f}" if price else "N/A"
            log.info(f"  {title}")
            log.info(f"    {count}/50 {outcome}  |  price: {price_str}")
            log.info("")


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
