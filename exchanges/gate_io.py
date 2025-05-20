import aiohttp
import ssl
import certifi
import logging
import asyncio
import time
from datetime import datetime, timezone
from typing import List, Dict, Optional
from utils.retry import retry_api

class RateLimiter:
    """Простой rate limiter для ограничения частоты запросов."""
    def __init__(self, rate: float, per: float):
        self.rate = rate
        self.per = per
        self.semaphore = asyncio.Semaphore(rate)
        self.last_reset = time.monotonic()
        self.available = rate

    async def acquire(self):
        """Ожидание разрешения на запрос."""
        async with self.semaphore:
            current = time.monotonic()
            elapsed = current - self.last_reset
            if elapsed >= self.per:
                self.available = self.rate
                self.last_reset = current
            if self.available <= 0:
                await asyncio.sleep(self.per - elapsed)
                self.available = self.rate
                self.last_reset = time.monotonic()
            self.available -= 1

class GateIOClient:
    """Клиент для работы с Gate.io API v4."""
    BASE_URL = "https://api.gateio.ws/api/v4"

    def __init__(self):
        """Инициализация клиента."""
        self.logger = logging.getLogger(__name__)
        self.rate_limiter = RateLimiter(rate=3, per=1.0)  # ~3 запроса/сек

    @retry_api()
    async def get_futures_pairs(self) -> List[Dict]:
        """Получение списка фьючерсных пар."""
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        pairs = []
        async with aiohttp.ClientSession() as session:
            try:
                await self.rate_limiter.acquire()
                async with session.get(
                    f"{self.BASE_URL}/futures/usdt/contracts",
                    ssl=ssl_context
                ) as resp:
                    data = await resp.json()
                    self.logger.debug(f"Gate.io API response: {data}")
                    if isinstance(data, dict) and "error" in data:
                        error = data.get("error", {})
                        self.logger.error(
                            f"Ошибка API Gate.io: label={error.get('label')}, "
                            f"message={error.get('message')}"
                        )
                        return []
                    self.logger.info(f"Получено {len(data)} контрактов с Gate.io")
                    for item in data:
                        name = item.get("name", "")
                        in_delisting = item.get("in_delisting", True)
                        quote_currency = name.split("_")[-1] if "_" in name else ""
                        self.logger.debug(
                            f"Gate.io instrument: contract={name}, "
                            f"in_delisting={in_delisting}, "
                            f"quote_currency={quote_currency}"
                        )
                        if not in_delisting and quote_currency == "USDT":
                            base_symbol = name.split("_")[0]
                            if base_symbol:
                                pairs.append({
                                    "symbol": name.replace("_", ""),
                                    "base_symbol": base_symbol
                                })
                            else:
                                self.logger.warning(f"Невалидный base_symbol для {name}")
                        else:
                            self.logger.debug(
                                f"Пара {name} отфильтрована: "
                                f"in_delisting={in_delisting}, quote_currency={quote_currency}"
                            )
                    self.logger.info(f"Отфильтровано {len(pairs)} фьючерсных пар с Gate.io")
            except aiohttp.ClientError as e:
                self.logger.error(f"Ошибка запроса к API Gate.io: {str(e)}")
                return []
        return pairs

    @retry_api()
    async def get_pair_data(self, pair_symbol: str, exchange: str) -> Optional[Dict]:
        """Получение данных по фьючерсной паре."""
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        gate_symbol = pair_symbol.replace("USDT", "_USDT")
        async with aiohttp.ClientSession() as session:
            for attempt in range(5):
                try:
                    # Тикер
                    await self.rate_limiter.acquire()
                    async with session.get(
                        f"{self.BASE_URL}/futures/usdt/tickers?contract={gate_symbol}",
                        ssl=ssl_context
                    ) as resp:
                        self.logger.debug(f"Gate.io ticker HTTP status for {pair_symbol}: {resp.status}")
                        data = await resp.json()
                        self.logger.debug(f"Gate.io API ticker response for {pair_symbol}: {data}")
                        if isinstance(data, dict) and "error" in data:
                            error = data.get("error", {})
                            if error.get("label") == "TOO_MANY_REQUESTS":
                                self.logger.warning(f"Лимит запросов для {pair_symbol}, ожидание...")
                                await asyncio.sleep(10)
                                continue
                            self.logger.error(
                                f"Ошибка API Gate.io для {pair_symbol}: "
                                f"label={error.get('label')}, message={error.get('message')}"
                            )
                            return None
                        if not data or not isinstance(data, list) or len(data) == 0:
                            self.logger.warning(f"Пустой ответ тикера для {pair_symbol}, попытка {attempt + 1}/5")
                            await asyncio.sleep(2)
                            continue
                        ticker = data[0]
                        price = float(ticker.get("last", 0))
                        volume_usd = float(ticker.get("volume_24h_quote", 0))
                        volume_btc = float(ticker.get("volume_24h_base", 0)) if pair_symbol == "BTCUSDT" else 0.0

                    # Открытый интерес
                    await self.rate_limiter.acquire()
                    async with session.get(
                        f"{self.BASE_URL}/futures/usdt/open_interest?contract={gate_symbol}",
                        ssl=ssl_context
                    ) as resp:
                        self.logger.debug(f"Gate.io open_interest HTTP status for {pair_symbol}: {resp.status}")
                        data = await resp.json()
                        self.logger.debug(f"Gate.io API open_interest response for {pair_symbol}: {data}")
                        if isinstance(data, dict) and "error" in data:
                            error = data.get("error", {})
                            if error.get("label") == "TOO_MANY_REQUESTS":
                                self.logger.warning(f"Лимит запросов для open_interest {pair_symbol}, ожидание...")
                                await asyncio.sleep(10)
                                continue
                            self.logger.error(
                                f"Ошибка получения open_interest для {pair_symbol}: "
                                f"label={error.get('label')}, message={error.get('message')}"
                            )
                            open_interest_contracts = 0.0
                        else:
                            open_interest_contracts = float(data.get("open_interest", 0))

                    # Funding rate
                    await self.rate_limiter.acquire()
                    async with session.get(
                        f"{self.BASE_URL}/futures/usdt/funding_rate?contract={gate_symbol}",
                        ssl=ssl_context
                    ) as resp:
                        self.logger.debug(f"Gate.io funding HTTP status for {pair_symbol}: {resp.status}")
                        data = await resp.json()
                        self.logger.debug(f"Gate.io API funding rate response for {pair_symbol}: {data}")
                        if isinstance(data, dict) and "error" in data:
                            error = data.get("error", {})
                            if error.get("label") == "TOO_MANY_REQUESTS":
                                self.logger.warning(f"Лимит запросов для фандинга {pair_symbol}, ожидание...")
                                await asyncio.sleep(10)
                                continue
                            self.logger.error(
                                f"Ошибка получения фандинга для {pair_symbol}: "
                                f"label={error.get('label')}, message={error.get('message')}"
                            )
                            return None
                        if not data or not isinstance(data, list):
                            self.logger.warning(f"Пустой ответ фандинга для {pair_symbol}, попытка {attempt + 1}/5")
                            await asyncio.sleep(2)
                            continue
                        latest_funding = max(data, key=lambda x: x.get("t", 0), default={})
                        funding_rate = float(latest_funding.get("r", 0))

                    base_symbol = pair_symbol.replace("USDT", "")
                    return {
                        "pair_symbol": pair_symbol,
                        "base_symbol": base_symbol,
                        "exchange": exchange,
                        "open_interest_contracts": open_interest_contracts,
                        "open_interest_usd": open_interest_contracts * price,
                        "funding_rate": funding_rate,
                        "volume_btc": volume_btc,
                        "volume_usd": volume_usd,
                        "price_usd": price,
                        "market_cap_usd": 0.0,
                        "timestamp": datetime.now(timezone.utc)
                    }
                except (aiohttp.ClientError, KeyError, ValueError) as e:
                    self.logger.error(f"Ошибка обработки данных для {pair_symbol}: {str(e)}")
                    return None
            self.logger.error(f"Не удалось получить данные для {pair_symbol} после 5 попыток")
            return None