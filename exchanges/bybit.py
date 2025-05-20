import aiohttp
import ssl
import certifi
import logging
from typing import List, Dict, Optional
from datetime import datetime, timezone
from utils.retry import retry_api


class BybitClient:
    """Клиент для работы с Bybit API."""

    BASE_URL = "https://api.bybit.com"

    def __init__(self):
        """Инициализация клиента."""
        self.logger = logging.getLogger(__name__)

    @retry_api()
    async def get_futures_pairs(self) -> List[Dict]:
        """Получение списка фьючерсных пар.

        Returns:
            List[Dict]: Список пар с ключами 'symbol' и 'base_symbol'.

        Raises:
            aiohttp.ClientError: Ошибка при запросе к API.
        """
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        async with aiohttp.ClientSession() as session:
            async with session.get(
                    f"{self.BASE_URL}/v5/market/tickers?category=linear",
                    ssl=ssl_context
            ) as resp:
                data = await resp.json()
                if "result" not in data or "list" not in data["result"]:
                    self.logger.error("Ошибка API Bybit: некорректный ответ")
                    return []
                pairs = []
                for item in data["result"]["list"]:
                    if item["symbol"].endswith("USDT"):
                        base_symbol = item["symbol"].replace("USDT", "")
                        pairs.append({
                            "symbol": item["symbol"],
                            "base_symbol": base_symbol
                        })
                self.logger.info(f"Получено {len(pairs)} фьючерсных пар с Bybit")
                return pairs

    @retry_api()
    async def get_pair_data(self, pair_symbol: str, exchange: str) -> Optional[Dict]:
        """Получение данных по фьючерсной паре.

        Args:
            pair_symbol (str): Символ пары (например, 'BTCUSDT').
            exchange (str): Название биржи ('bybit').

        Returns:
            Optional[Dict]: Данные о паре или None при ошибке.

        Raises:
            aiohttp.ClientError: Ошибка при запросе к API.
        """
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        async with aiohttp.ClientSession() as session:
            try:
                # OI, цена, объем
                async with session.get(
                        f"{self.BASE_URL}/v5/market/tickers?category=linear&symbol={pair_symbol}",
                        ssl=ssl_context
                ) as resp:
                    data = await resp.json()
                    if "result" not in data or not data["result"]["list"]:
                        self.logger.error(f"Ошибка API Bybit для {pair_symbol}: некорректный ответ")
                        return None
                    ticker = data["result"]["list"][0]
                    price = float(ticker["lastPrice"])
                    oi_contracts = float(ticker["openInterest"])
                    volume_usd = float(ticker["volume24h"]) * price
                    volume_btc = float(ticker["volume24h"]) if pair_symbol == "BTCUSDT" else 0.0

                # Фандинговая ставка
                async with session.get(
                        f"{self.BASE_URL}/v5/market/funding/history?category=linear&symbol={pair_symbol}&limit=1",
                        ssl=ssl_context
                ) as resp:
                    data = await resp.json()
                    if "result" not in data or not data["result"]["list"]:
                        self.logger.error(f"Ошибка получения фандинга для {pair_symbol}")
                        return None
                    funding_rate = float(data["result"]["list"][0]["fundingRate"])

                base_symbol = pair_symbol.replace("USDT", "")

                return {
                    "pair_symbol": pair_symbol,
                    "base_symbol": base_symbol,
                    "exchange": exchange,
                    "timestamp": datetime.now(timezone.utc),
                    "open_interest_contracts": oi_contracts,
                    "open_interest_usd": oi_contracts * price,
                    "funding_rate": funding_rate,
                    "volume_btc": volume_btc,
                    "volume_usd": volume_usd,
                    "price_usd": price,
                    "market_cap_usd": 0.0  # Bybit не предоставляет market cap
                }
            except (aiohttp.ClientError, KeyError, ValueError) as e:
                self.logger.error(f"Ошибка обработки данных для {pair_symbol}: {str(e)}")
                return None