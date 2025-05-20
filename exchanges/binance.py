import aiohttp
import ssl
import certifi
import logging
from typing import List, Dict, Optional
from datetime import datetime, timezone
from utils.retry import retry_api


class BinanceClient:
    """Клиент для работы с Binance API."""

    BASE_URL = "https://fapi.binance.com"

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
                    f"{self.BASE_URL}/fapi/v1/exchangeInfo",
                    ssl=ssl_context
            ) as resp:
                data = await resp.json()
                if "symbols" not in data:
                    self.logger.error("Ошибка API Binance: отсутствует ключ 'symbols'")
                    return []
                pairs = []
                for symbol in data["symbols"]:
                    if symbol["contractType"] == "PERPETUAL":
                        base_symbol = symbol["baseAsset"]
                        pairs.append({
                            "symbol": symbol["symbol"],
                            "base_symbol": base_symbol
                        })
                self.logger.info(f"Получено {len(pairs)} фьючерсных пар с Binance")
                return pairs

    @retry_api()
    async def get_pair_data(self, pair_symbol: str, exchange: str) -> Optional[Dict]:
        """Получение данных по паре.

        Args:
            pair_symbol (str): Символ пары (например, 'BTCUSDT').
            exchange (str): Название биржи ('binance').

        Returns:
            Optional[Dict]: Данные о паре или None при ошибке.

        Raises:
            aiohttp.ClientError: Ошибка при запросе к API.
        """
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        async with aiohttp.ClientSession() as session:
            try:
                # OI
                async with session.get(
                        f"{self.BASE_URL}/fapi/v1/openInterest?symbol={pair_symbol}",
                        ssl=ssl_context
                ) as resp:
                    oi_data = await resp.json()
                    oi_contracts = float(oi_data["openInterest"])

                # Цена контракта
                async with session.get(
                        f"{self.BASE_URL}/fapi/v1/ticker/price?symbol={pair_symbol}",
                        ssl=ssl_context
                ) as resp:
                    price_data = await resp.json()
                    price = float(price_data["price"])

                # Фандинговая ставка
                async with session.get(
                        f"{self.BASE_URL}/fapi/v1/premiumIndex?symbol={pair_symbol}",
                        ssl=ssl_context
                ) as resp:
                    funding_data = await resp.json()
                    funding_rate = float(funding_data["lastFundingRate"])

                # Объем
                async with session.get(
                        f"{self.BASE_URL}/fapi/v1/ticker/24hr?symbol={pair_symbol}",
                        ssl=ssl_context
                ) as resp:
                    volume_data = await resp.json()
                    volume_usd = float(volume_data["quoteVolume"])
                    volume_btc = float(volume_data["volume"]) if pair_symbol == "BTCUSDT" else 0.0

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
                    "market_cap_usd": 0.0  # Binance не предоставляет market cap
                }
            except (aiohttp.ClientError, KeyError, ValueError) as e:
                self.logger.error(f"Ошибка получения данных для {pair_symbol}: {str(e)}")
                return None