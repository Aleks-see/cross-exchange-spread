import asyncio
import json
import time
from typing import Dict, Any

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")


# ============================================================
# SYMBOLS
# ============================================================

def normalize_symbol(symbol: str) -> str:
    """
    Пользовательский символ всегда приводим к виду:
    NILUSDT
    BTCUSDT
    XRPUSDT
    """
    s = symbol.strip().upper().replace("-", "").replace("/", "")

    if s.endswith("USDT"):
        return s

    return s + "USDT"


def exchange_symbol(exchange: str, symbol: str) -> str:
    """
    Преобразование одного и того же пользовательского символа
    в формат конкретной биржи.
    """

    s = normalize_symbol(symbol)
    base = s[:-4]

    if exchange in ("MEXC", "GATE"):
        return f"{base}_USDT"

    # Binance / Bybit
    return f"{base}USDT"


# ============================================================
# QUOTE
# ============================================================

def make_quote(
    exchange: str,
    original_symbol: str,
    bid: float,
    ask: float
) -> Dict[str, Any]:

    mid = (bid + ask) / 2.0

    return {
        "exchange": exchange,
        "symbol": normalize_symbol(original_symbol),
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "time": time.time(),
    }


# ============================================================
# BINANCE
# ============================================================

async def binance_connection(
    symbol: str,
    queue: asyncio.Queue
):

    exchange = "BINANCE"
    ws_symbol = exchange_symbol(exchange, symbol).lower()

    url = f"wss://fstream.binance.com/ws/{ws_symbol}@bookTicker"

    while True:

        try:

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
            ) as ws:

                while True:

                    raw = await ws.recv()
                    data = json.loads(raw)

                    # Проверяем реальный символ от Binance.
                    real_symbol = str(data.get("s", "")).upper()

                    if real_symbol != ws_symbol.upper():
                        continue

                    bid = float(data["b"])
                    ask = float(data["a"])

                    if bid <= 0 or ask <= 0:
                        continue

                    await queue.put(
                        make_quote(
                            exchange,
                            symbol,
                            bid,
                            ask
                        )
                    )

        except asyncio.CancelledError:
            raise

        except Exception:
            await asyncio.sleep(2)


# ============================================================
# BYBIT
# ============================================================

async def bybit_connection(
    symbol: str,
    queue: asyncio.Queue
):

    exchange = "BYBIT"
    ws_symbol = exchange_symbol(exchange, symbol)

    url = "wss://stream.bybit.com/v5/public/linear"

    while True:

        try:

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
            ) as ws:

                subscribe_message = {
                    "op": "subscribe",
                    "args": [
                        f"tickers.{ws_symbol}"
                    ]
                }

                await ws.send(
                    json.dumps(subscribe_message)
                )

                while True:

                    raw = await ws.recv()
                    data = json.loads(raw)

                    topic = data.get("topic", "")

                    if topic != f"tickers.{ws_symbol}":
                        continue

                    ticker = data.get("data")

                    if not isinstance(ticker, dict):
                        continue

                    real_symbol = str(
                        ticker.get("symbol", "")
                    ).upper()

                    if real_symbol != ws_symbol.upper():
                        continue

                    bid_raw = ticker.get("bid1Price")
                    ask_raw = ticker.get("ask1Price")

                    if bid_raw is None or ask_raw is None:
                        continue

                    bid = float(bid_raw)
                    ask = float(ask_raw)

                    if bid <= 0 or ask <= 0:
                        continue

                    await queue.put(
                        make_quote(
                            exchange,
                            symbol,
                            bid,
                            ask
                        )
                    )

        except asyncio.CancelledError:
            raise

        except Exception:
            await asyncio.sleep(2)


# ============================================================
# MEXC
# ============================================================

async def mexc_connection(
    symbol: str,
    queue: asyncio.Queue
):

    exchange = "MEXC"
    ws_symbol = exchange_symbol(exchange, symbol)

    url = "wss://contract.mexc.com/edge"

    while True:

        try:

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
            ) as ws:

                subscribe_message = {
                    "method": "sub.ticker",
                    "param": {
                        "symbol": ws_symbol
                    }
                }

                await ws.send(
                    json.dumps(subscribe_message)
                )

                async def mexc_ping():

                    while True:

                        await asyncio.sleep(15)

                        try:
                            await ws.send(
                                json.dumps({
                                    "method": "ping"
                                })
                            )
                        except Exception:
                            return

                ping_task = asyncio.create_task(
                    mexc_ping()
                )

                try:

                    while True:

                        raw = await ws.recv()
                        data = json.loads(raw)

                        if data.get("channel") != "push.ticker":
                            continue

                        ticker = data.get("data")

                        if not isinstance(ticker, dict):
                            continue

                        real_symbol = str(
                            ticker.get("symbol", "")
                        ).upper()

                        if real_symbol != ws_symbol.upper():
                            continue

                        bid_raw = ticker.get("bid1")
                        ask_raw = ticker.get("ask1")

                        if bid_raw is None or ask_raw is None:
                            continue

                        bid = float(bid_raw)
                        ask = float(ask_raw)

                        if bid <= 0 or ask <= 0:
                            continue

                        await queue.put(
                            make_quote(
                                exchange,
                                symbol,
                                bid,
                                ask
                            )
                        )

                finally:

                    ping_task.cancel()

                    try:
                        await ping_task
                    except asyncio.CancelledError:
                        pass

        except asyncio.CancelledError:
            raise

        except Exception:
            await asyncio.sleep(2)


# ============================================================
# GATE
# ============================================================

async def gate_connection(
    symbol: str,
    queue: asyncio.Queue
):

    exchange = "GATE"
    ws_symbol = exchange_symbol(exchange, symbol)

    # ВАЖНО:
    # именно USDT endpoint.
    # Старый общий endpoint мог отправлять в BTC-контракты.
    url = "wss://fx-ws.gateio.ws/v4/ws/usdt"

    while True:

        try:

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
            ) as ws:

                subscribe_message = {
                    "time": int(time.time()),
                    "channel": "futures.book_ticker",
                    "event": "subscribe",
                    "payload": [
                        ws_symbol
                    ]
                }

                await ws.send(
                    json.dumps(subscribe_message)
                )

                while True:

                    raw = await ws.recv()
                    data = json.loads(raw)

                    if data.get("channel") != "futures.book_ticker":
                        continue

                    if data.get("event") != "update":
                        continue

                    result = data.get("result")

                    if not isinstance(result, dict):
                        continue

                    # =================================================
                    # КРИТИЧЕСКАЯ ПРОВЕРКА
                    #
                    # Если мы запросили NIL_USDT,
                    # BTC_USDT сюда никогда не попадёт.
                    # =================================================

                    real_symbol = str(
                        result.get("s", "")
                    ).upper()

                    if real_symbol != ws_symbol.upper():
                        continue

                    bid_raw = result.get("b")
                    ask_raw = result.get("a")

                    if bid_raw in (None, ""):
                        continue

                    if ask_raw in (None, ""):
                        continue

                    bid = float(bid_raw)
                    ask = float(ask_raw)

                    if bid <= 0 or ask <= 0:
                        continue

                    await queue.put(
                        make_quote(
                            exchange,
                            symbol,
                            bid,
                            ask
                        )
                    )

        except asyncio.CancelledError:
            raise

        except Exception:
            await asyncio.sleep(2)


# ============================================================
# EXCHANGE ROUTER
# ============================================================

EXCHANGE_CONNECTIONS = {
    "BINANCE": binance_connection,
    "BYBIT": bybit_connection,
    "MEXC": mexc_connection,
    "GATE": gate_connection,
}


# ============================================================
# ROOT
# ============================================================

@app.get("/")
async def root():

    return FileResponse(
        "static/index.html"
    )


# ============================================================
# WEBSOCKET
# ============================================================

@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket
):

    await websocket.accept()

    tasks = []

    try:

        while True:

            raw = await websocket.receive_text()

            try:
                config = json.loads(raw)
            except Exception:
                continue

            # --------------------------------------------------------
            # Поддерживаем несколько возможных названий полей,
            # чтобы не ломать существующий frontend.
            # --------------------------------------------------------

            exchange1 = (
                config.get("exchange1")
                or config.get("firstExchange")
                or config.get("exchange")
            )

            exchange2 = (
                config.get("exchange2")
                or config.get("secondExchange")
            )

            symbol1 = (
                config.get("symbol1")
                or config.get("symbol")
                or config.get("ticker")
            )

            symbol2 = (
                config.get("symbol2")
                or symbol1
            )

            if not exchange1 or not exchange2:
                continue

            if not symbol1 or not symbol2:
                continue

            exchange1 = str(exchange1).upper().strip()
            exchange2 = str(exchange2).upper().strip()

            symbol1 = normalize_symbol(str(symbol1))
            symbol2 = normalize_symbol(str(symbol2))

            if exchange1 not in EXCHANGE_CONNECTIONS:
                continue

            if exchange2 not in EXCHANGE_CONNECTIONS:
                continue

            # --------------------------------------------------------
            # Останавливаем старые потоки этого клиента.
            # --------------------------------------------------------

            for task in tasks:
                task.cancel()

            if tasks:

                await asyncio.gather(
                    *tasks,
                    return_exceptions=True
                )

            tasks = []

            queue = asyncio.Queue()

            quote1 = None
            quote2 = None

            # --------------------------------------------------------
            # Запускаем ДВА независимых потока.
            # Каждый получает СВОЙ exchange + СВОЙ symbol.
            # --------------------------------------------------------

            task1 = asyncio.create_task(
                EXCHANGE_CONNECTIONS[exchange1](
                    symbol1,
                    queue
                )
            )

            task2 = asyncio.create_task(
                EXCHANGE_CONNECTIONS[exchange2](
                    symbol2,
                    queue
                )
            )

            tasks = [task1, task2]

            # --------------------------------------------------------
            # Сообщаем frontend, какие реальные инструменты выбраны.
            # --------------------------------------------------------

            await websocket.send(
                json.dumps({
                    "type": "config",
                    "exchange1": exchange1,
                    "symbol1": symbol1,
                    "exchange2": exchange2,
                    "symbol2": symbol2,
                })
            )

            # --------------------------------------------------------
            # Получаем котировки.
            # --------------------------------------------------------

            while True:

                quote = await queue.get()

                if quote["exchange"] == exchange1:
                    quote1 = quote

                elif quote["exchange"] == exchange2:
                    quote2 = quote

                # ----------------------------------------------------
                # Пока обе цены не пришли — spread не считаем.
                # ----------------------------------------------------

                if quote1 is None or quote2 is None:
                    await websocket.send(
                        json.dumps({
                            "type": "quote",
                            **quote,
                        })
                    )
                    continue

                mid1 = quote1["mid"]
                mid2 = quote2["mid"]

                if mid1 <= 0 or mid2 <= 0:
                    continue

                # ----------------------------------------------------
                # ОСНОВНОЙ SPREAD
                #
                # + = первый инструмент дороже второго
                # - = первый инструмент дешевле второго
                # ----------------------------------------------------

                spread = (
                    (mid1 / mid2) - 1.0
                ) * 100.0

                # ----------------------------------------------------
                # Отправляем одновременно:
                # 1. обе котировки
                # 2. реальные символы
                # 3. spread
                #
                # symbol1 и symbol2 всегда пользовательские символы.
                # Поэтому GATE больше не сможет показывать BTCUSDT,
                # если пользователь выбрал NILUSDT.
                # ----------------------------------------------------

                response = {
                    "type": "spread",

                    "exchange1": exchange1,
                    "symbol1": symbol1,

                    "exchange2": exchange2,
                    "symbol2": symbol2,

                    "bid1": quote1["bid"],
                    "ask1": quote1["ask"],
                    "mid1": mid1,

                    "bid2": quote2["bid"],
                    "ask2": quote2["ask"],
                    "mid2": mid2,

                    "spread": spread,

                    # Дополнительно оставляем структуры quote,
                    # чтобы frontend мог использовать их напрямую.
                    "first": quote1,
                    "second": quote2,
                }

                await websocket.send(
                    json.dumps(response)
                )

    except WebSocketDisconnect:
        pass

    except asyncio.CancelledError:
        raise

    finally:

        for task in tasks:
            task.cancel()

        if tasks:

            await asyncio.gather(
                *tasks,
                return_exceptions=True
            )
