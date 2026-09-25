# Cross-Exchange Futures Spread

Cloud web app for real-time Bid/Ask spread monitoring on:
- Binance Futures
- Bybit Futures
- MEXC Futures
- Gate Futures

## Spread convention

The chart uses the midpoint of the live best Bid/Ask:
`mid = (Bid + Ask) / 2`

`spread = (mid_first / mid_second - 1) * 100`

Therefore:
- negative spread = first exchange is cheaper -> BUY first / SELL second
- positive spread = first exchange is more expensive -> SELL first / BUY second

The displayed Bid/Ask are the actual best prices received from each exchange.

## Run locally

```bash
pip install -r requirements.txt
uvicorn app:app --reload
```

Open http://127.0.0.1:8000

## Cloud

Deploy as a Python web service from this repository. The start command is already in `Procfile`.
No API keys are required because this first version uses public market data only.
