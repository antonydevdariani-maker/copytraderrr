# Polymarket Consensus Trader

Finds markets where ≥15 of the top 50 Polymarket traders (by monthly PNL) share the same position. Runs every 30 minutes.

## Setup

```bash
pip install -r requirements.txt
python polymarket_consensus.py
```

Results print to console and append to `consensus_log.txt`.
