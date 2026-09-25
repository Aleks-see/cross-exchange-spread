import asyncio, json, time
from typing import Dict, Any
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

clients = set()
market_tasks: Dict[str, asyncio.Task] = {}
latest: Dict[str, Dict[str, float]] = {}

EXCHANGE_INFO = {
    "BINANCE": "binance",
    "BYBIT": "bybit",
    "MEXC": "mexc",
    "GATE": "gate",
}

def normalize_symbol(symbol: str) -> str:
    s = symbol.strip().upper().replace("-", "").replace("/", "")
    if s.endswith("USDT"):
        return s
    return s + "USDT"

def exchange_symbol(exchange: str, symbol: str) -> str:
    s = normalize_symbol(symbol)
    base = s[:-4]
    if exchange in ("MEXC", "GATE"):
        return f"{base}_USDT"
    return s

def key(exchange: str, symbol: str) -> str:
    return f"{exchange}:{normalize_symbol(symbol)}"

async def publish(exchange: str, symbol: str, bid: float, ask: float, ts=None):
    if bid <= 0 or ask <= 0:
        return
    k = key(exchange, symbol)
    latest[k] = {"bid": bid, "ask": ask, "ts": ts or int(time.time() * 1000)}
    msg = {"type": "quote", "exchange": exchange, "symbol": normalize_symbol(symbol),
           "bid": bid, "ask": ask, "ts": latest[k]["ts"]}
    dead = []
    for ws in list(clients):
        try:
            await ws.send_json(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)

async def binance(symbol: str):
    sym = normalize_symbol(symbol).lower()
    url = f"wss://fstream.binance.com/public/ws/{sym}@bookTicker"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                async for raw in ws:
                    d = json.loads(raw)
                    # Binance bookTicker fields: b=best bid, a=best ask, E=event time
                    if "b" in d and "a" in d:
                        await publish("BINANCE", symbol, float(d["b"]), float(d["a"]), d.get("E"))
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2)

async def bybit(symbol: str):
    sym = normalize_symbol(symbol)
    url = "wss://stream.bybit.com/v5/public/linear"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": [f"tickers.{sym}"]}))
                async for raw in ws:
                    d = json.loads(raw)
                    data = d.get("data", {})
                    if d.get("topic", "").startswith("tickers.") and data:
                        bid = float(data.get("bid1Price", 0) or 0)
                        ask = float(data.get("ask1Price", 0) or 0)
                        if bid and ask:
                            await publish("BYBIT", symbol, bid, ask, d.get("ts"))
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2)

async def mexc(symbol: str):
    sym = exchange_symbol("MEXC", symbol)
    url = "wss://contract.mexc.com/edge"
    while True:
        try:
            async with websockets.connect(url, ping_interval=None) as ws:
                await ws.send(json.dumps({"method": "sub.ticker", "param": {"symbol": sym}}))
                async for raw in ws:
                    d = json.loads(raw)
                    if d.get("channel") == "push.ticker":
                        data = d.get("data", {})
                        bid = float(data.get("bid1", 0) or 0)
                        ask = float(data.get("ask1", 0) or 0)
                        if bid and ask:
                            await publish("MEXC", symbol, bid, ask, data.get("timestamp"))
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2)

async def gate(symbol: str):
    sym = exchange_symbol("GATE", symbol)
    url = "wss://fx-ws.gateio.ws/v4/ws/usdt"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20,
                                          additional_headers={"X-Gate-Size-Decimal": "1"}) as ws:
                req = {"time": int(time.time()), "channel": "futures.book_ticker",
                       "event": "subscribe", "payload": [sym]}
                await ws.send(json.dumps(req))
                async for raw in ws:
                    d = json.loads(raw)
                    if d.get("channel") == "futures.book_ticker" and d.get("event") == "update":
                        r = d.get("result", {})
                        bid = float(r.get("b", 0) or 0)
                        ask = float(r.get("a", 0) or 0)
                        if bid and ask:
                            await publish("GATE", symbol, bid, ask, r.get("t"))
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2)

FACTORIES = {"BINANCE": binance, "BYBIT": bybit, "MEXC": mexc, "GATE": gate}

async def stop_all():
    for t in list(market_tasks.values()):
        t.cancel()
    if market_tasks:
        await asyncio.gather(*market_tasks.values(), return_exceptions=True)
    market_tasks.clear()

async def start_pair(ex1, ex2, symbol):
    await stop_all()
    for ex in (ex1, ex2):
        market_tasks[ex] = asyncio.create_task(FACTORIES[ex](symbol))

@app.get("/")
async def index():
    return FileResponse("static/index.html")

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        await ws.send_json({"type": "status", "message": "connected"})
        while True:
            msg = await ws.receive_json()
            if msg.get("type") == "config":
                ex1 = msg.get("exchange1", "GATE").upper()
                ex2 = msg.get("exchange2", "BINANCE").upper()
                symbol = normalize_symbol(msg.get("symbol", "BTCUSDT"))
                if ex1 == ex2:
                    await ws.send_json({"type": "error", "message": "Выберите две разные биржи."})
                    continue
                await start_pair(ex1, ex2, symbol)
                await ws.send_json({"type": "status", "message": f"{ex1} / {ex2} — {symbol}"})
    except WebSocketDisconnect:
        clients.discard(ws)
    except Exception:
        clients.discard(ws)

@app.on_event("shutdown")
async def shutdown():
    await stop_all()
