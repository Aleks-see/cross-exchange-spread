import asyncio
import json
import time
from typing import Any, Dict, Optional

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")


def normalize_symbol(symbol: str) -> str:
    s = symbol.strip().upper().replace("-", "").replace("/", "").replace("_", "")
    if not s:
        return ""
    return s if s.endswith("USDT") else s + "USDT"


def base_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    return s[:-4] if s.endswith("USDT") else s


def exchange_symbol(exchange: str, symbol: str) -> str:
    base = base_symbol(symbol)

    if exchange in ("MEXC", "GATE"):
        return f"{base}_USDT"

    return f"{base}USDT"


def to_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None

        x = float(value)

        if x <= 0:
            return None

        return x

    except (TypeError, ValueError):
        return None


async def send_json(
    ws: WebSocket,
    lock: asyncio.Lock,
    payload: Dict[str, Any]
) -> bool:

    try:
        async with lock:
            await ws.send_text(
                json.dumps(payload, ensure_ascii=False)
            )

        return True

    except Exception:
        return False


async def send_error(
    ws: WebSocket,
    lock: asyncio.Lock,
    message: str
) -> None:

    await send_json(
        ws,
        lock,
        {
            "type": "error",
            "message": message
        }
    )


# ============================================================
# BINANCE
# ============================================================

async def binance_worker(
    ws: WebSocket,
    lock: asyncio.Lock,
    symbol: str,
    quotes: Dict[str, Dict[str, float]],
    publish
):

    exchange = "BINANCE"

    ex_symbol = exchange_symbol(
        exchange,
        symbol
    ).lower()

    url = (
        f"wss://fstream.binance.com/ws/"
        f"{ex_symbol}@bookTicker"
    )

    while True:

        try:

            await send_error(
                ws,
                lock,
                f"{exchange}: подключение {ex_symbol.upper()}"
            )

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=15,
                close_timeout=5
            ) as sock:

                await send_error(
                    ws,
                    lock,
                    f"{exchange}: WebSocket подключен"
                )

                async for raw in sock:

                    try:
                        msg = json.loads(raw)

                    except Exception:
                        continue

                    data = msg.get(
                        "data",
                        msg
                    )

                    bid = to_float(
                        data.get("b")
                    )

                    ask = to_float(
                        data.get("a")
                    )

                    if bid is not None and ask is not None:

                        quotes[exchange] = {
                            "bid": bid,
                            "ask": ask
                        }

                        await publish()

        except asyncio.CancelledError:
            raise

        except Exception as e:

            await send_error(
                ws,
                lock,
                f"{exchange}: {type(e).__name__}: {e}"
            )

            await asyncio.sleep(3)


# ============================================================
# BYBIT
# ============================================================

async def bybit_worker(
    ws: WebSocket,
    lock: asyncio.Lock,
    symbol: str,
    quotes: Dict[str, Dict[str, float]],
    publish
):

    exchange = "BYBIT"

    ex_symbol = exchange_symbol(
        exchange,
        symbol
    )

    url = (
        "wss://stream.bybit.com/"
        "v5/public/linear"
    )

    last_bid = None
    last_ask = None

    while True:

        try:

            await send_error(
                ws,
                lock,
                f"{exchange}: подключение {ex_symbol}"
            )

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=15,
                close_timeout=5
            ) as sock:

                await sock.send(
                    json.dumps(
                        {
                            "op": "subscribe",
                            "args": [
                                f"tickers.{ex_symbol}"
                            ]
                        }
                    )
                )

                await send_error(
                    ws,
                    lock,
                    f"{exchange}: подписка tickers.{ex_symbol}"
                )

                async for raw in sock:

                    try:
                        msg = json.loads(raw)

                    except Exception:
                        continue

                    if msg.get("success") is False:

                        await send_error(
                            ws,
                            lock,
                            f"{exchange}: ошибка подписки: {msg}"
                        )

                        continue

                    data = msg.get("data")

                    if isinstance(data, list):

                        data = (
                            data[0]
                            if data
                            else None
                        )

                    if not isinstance(data, dict):
                        continue

                    bid = to_float(
                        data.get("bid1Price")
                    )

                    ask = to_float(
                        data.get("ask1Price")
                    )

                    if bid is not None:
                        last_bid = bid

                    if ask is not None:
                        last_ask = ask

                    if (
                        last_bid is not None
                        and
                        last_ask is not None
                    ):

                        quotes[exchange] = {
                            "bid": last_bid,
                            "ask": last_ask
                        }

                        await publish()

        except asyncio.CancelledError:
            raise

        except Exception as e:

            await send_error(
                ws,
                lock,
                f"{exchange}: {type(e).__name__}: {e}"
            )

            await asyncio.sleep(3)


# ============================================================
# MEXC
# ============================================================

async def mexc_worker(
    ws: WebSocket,
    lock: asyncio.Lock,
    symbol: str,
    quotes: Dict[str, Dict[str, float]],
    publish
):

    exchange = "MEXC"

    ex_symbol = exchange_symbol(
        exchange,
        symbol
    )

    url = "wss://contract.mexc.com/edge"

    while True:

        try:

            await send_error(
                ws,
                lock,
                f"{exchange}: подключение {ex_symbol}"
            )

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=15,
                close_timeout=5
            ) as sock:

                await sock.send(
                    json.dumps(
                        {
                            "method": "sub.ticker",
                            "param": {
                                "symbol": ex_symbol
                            }
                        }
                    )
                )

                await send_error(
                    ws,
                    lock,
                    f"{exchange}: подписка sub.ticker {ex_symbol}"
                )

                async for raw in sock:

                    try:
                        msg = json.loads(raw)

                    except Exception:
                        continue

                    if msg.get("channel") != "push.ticker":
                        continue

                    data = msg.get("data")

                    if not isinstance(data, dict):
                        continue

                    bid = to_float(
                        data.get("bid1")
                    )

                    ask = to_float(
                        data.get("ask1")
                    )

                    if bid is not None and ask is not None:

                        quotes[exchange] = {
                            "bid": bid,
                            "ask": ask
                        }

                        await publish()

        except asyncio.CancelledError:
            raise

        except Exception as e:

            await send_error(
                ws,
                lock,
                f"{exchange}: {type(e).__name__}: {e}"
            )

            await asyncio.sleep(3)


# ============================================================
# GATE
# ============================================================

async def gate_worker(
    ws: WebSocket,
    lock: asyncio.Lock,
    symbol: str,
    quotes: Dict[str, Dict[str, float]],
    publish
):

    exchange = "GATE"

    ex_symbol = exchange_symbol(
        exchange,
        symbol
    )

    url = (
        "wss://fx-ws.gateio.ws/"
        "v4/ws/usdt"
    )

    while True:

        try:

            await send_error(
                ws,
                lock,
                f"{exchange}: подключение {ex_symbol}"
            )

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=15,
                close_timeout=5
            ) as sock:

                await sock.send(
                    json.dumps(
                        {
                            "time": int(time.time()),
                            "channel": "futures.book_ticker",
                            "event": "subscribe",
                            "payload": [
                                ex_symbol
                            ]
                        }
                    )
                )

                await send_error(
                    ws,
                    lock,
                    f"{exchange}: подписка futures.book_ticker {ex_symbol}"
                )

                async for raw in sock:

                    try:
                        msg = json.loads(raw)

                    except Exception:
                        continue

                    if msg.get("error"):

                        await send_error(
                            ws,
                            lock,
                            f"{exchange}: API error: {msg.get('error')}"
                        )

                        continue

                    result = msg.get("result")

                    if not isinstance(result, dict):
                        continue

                    bid = to_float(
                        result.get("b")
                    )

                    ask = to_float(
                        result.get("a")
                    )

                    if bid is not None and ask is not None:

                        quotes[exchange] = {
                            "bid": bid,
                            "ask": ask
                        }

                        await publish()

        except asyncio.CancelledError:
            raise

        except Exception as e:

            await send_error(
                ws,
                lock,
                f"{exchange}: {type(e).__name__}: {e}"
            )

            await asyncio.sleep(3)


# ============================================================
# EXCHANGE SELECTOR
# ============================================================

async def run_exchange(
    exchange: str,
    ws: WebSocket,
    lock: asyncio.Lock,
    symbol: str,
    quotes: Dict[str, Dict[str, float]],
    publish
):

    if exchange == "BINANCE":

        return await binance_worker(
            ws,
            lock,
            symbol,
            quotes,
            publish
        )

    if exchange == "BYBIT":

        return await bybit_worker(
            ws,
            lock,
            symbol,
            quotes,
            publish
        )

    if exchange == "MEXC":

        return await mexc_worker(
            ws,
            lock,
            symbol,
            quotes,
            publish
        )

    if exchange == "GATE":

        return await gate_worker(
            ws,
            lock,
            symbol,
            quotes,
            publish
        )

    await send_error(
        ws,
        lock,
        f"Неизвестная биржа: {exchange}"
    )


# ============================================================
# MAIN PAGE
# ============================================================

@app.get("/")
async def index():

    return FileResponse(
        "static/index.html"
    )


# ============================================================
# BROWSER WEBSOCKET
# ============================================================

@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket
):

    await websocket.accept()

    lock = asyncio.Lock()

    tasks = []

    try:

        raw = await websocket.receive_text()

        config = json.loads(raw)

        if config.get("type") != "config":

            await send_error(
                websocket,
                lock,
                "Ожидалась конфигурация подключения."
            )

            return

        exchange1 = str(
            config.get(
                "exchange1",
                ""
            )
        ).upper()

        exchange2 = str(
            config.get(
                "exchange2",
                ""
            )
        ).upper()

        symbol = normalize_symbol(
            str(
                config.get("symbol1")
                or
                config.get("symbol2")
                or
                ""
            )
        )

        allowed = {
            "BINANCE",
            "BYBIT",
            "MEXC",
            "GATE"
        }

        if exchange1 not in allowed:

            await send_error(
                websocket,
                lock,
                f"Неизвестная первая биржа: {exchange1}"
            )

            return

        if exchange2 not in allowed:

            await send_error(
                websocket,
                lock,
                f"Неизвестная вторая биржа: {exchange2}"
            )

            return

        if exchange1 == exchange2:

            await send_error(
                websocket,
                lock,
                "Выберите две разные биржи."
            )

            return

        if not symbol:

            await send_error(
                websocket,
                lock,
                "Введите тикер."
            )

            return

        await send_json(
            websocket,
            lock,
            {
                "type": "config",
                "exchange1": exchange1,
                "exchange2": exchange2,
                "symbol1": symbol,
                "symbol2": symbol
            }
        )

        quotes: Dict[str, Dict[str, float]] = {}

        async def publish():

            q1 = quotes.get(exchange1)
            q2 = quotes.get(exchange2)

            if not q1 or not q2:
                return

            bid1 = q1["bid"]
            ask1 = q1["ask"]

            bid2 = q2["bid"]
            ask2 = q2["ask"]

            mid1 = (
                bid1 + ask1
            ) / 2.0

            mid2 = (
                bid2 + ask2
            ) / 2.0

            if mid2 == 0:
                return

            spread = (
                mid1 / mid2 - 1.0
            ) * 100.0

            await send_json(
                websocket,
                lock,
                {
                    "type": "spread",

                    "exchange1": exchange1,
                    "exchange2": exchange2,

                    "symbol1": symbol,
                    "symbol2": symbol,

                    "bid1": bid1,
                    "ask1": ask1,
                    "mid1": mid1,

                    "bid2": bid2,
                    "ask2": ask2,
                    "mid2": mid2,

                    "spread": spread
                }
            )

        tasks = [

            asyncio.create_task(
                run_exchange(
                    exchange1,
                    websocket,
                    lock,
                    symbol,
                    quotes,
                    publish
                )
            ),

            asyncio.create_task(
                run_exchange(
                    exchange2,
                    websocket,
                    lock,
                    symbol,
                    quotes,
                    publish
                )
            )
        ]

        await send_error(
            websocket,
            lock,
            f"Запуск: {exchange1} ↔ {exchange2}, тикер {symbol}"
        )

        await asyncio.gather(
            *tasks
        )

    except WebSocketDisconnect:

        pass

    except asyncio.CancelledError:

        raise

    except Exception as e:

        try:

            await send_error(
                websocket,
                lock,
                f"SERVER: {type(e).__name__}: {e}"
            )

        except Exception:

            pass

    finally:

        for task in tasks:
            task.cancel()

        if tasks:

            await asyncio.gather(
                *tasks,
                return_exceptions=True
            )
