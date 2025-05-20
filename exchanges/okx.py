import aiohttp
import ssl
import certifi
import logging
from datetime import datetime, timezone
from typing import List, Dict, Optional
from utils.retry import retry_api


class OKXClient:
    """Клиент для работы с OKX API."""

    BASE_URL = "https://www.okx.com"

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
        pairs = []
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(
                        f"{self.BASE_URL}/api/v5/public/instruments?instType=FUTURES",
                        ssl=ssl_context
                ) as resp:
                    data = await resp.json()
                    self.logger.debug(f"OKX API response: {data}")
                    if data.get("code") != "0":
                        self.logger.error(f"Ошибка API OKX: {data.get('msg')}")
                        return []
                    for item in data.get("data", []):
                        self.logger.debug(
                            f"OKX instrument: instId={item.get('instId')}, "
                            f"uly={item.get('uly')}, state={item.get('state')}"
                        )
                        # Убираем фильтрацию по state, чтобы включить все активные пары
                        if item.get("instType") == "FUTURES":
                            base_symbol = item.get("uly", "").split("-")[0]
                            if base_symbol:  # Проверяем, что base_symbol не пустой
                                pairs.append({
                                    "symbol": item["instId"],
                                    "base_symbol": base_symbol
                                })
            except aiohttp.ClientError as e:
                self.logger.error(f"Ошибка запроса к API OKX: {str(e)}")
                return []
        self.logger.info(f"Получено {len(pairs)} фьючерсных пар с OKX")
        return pairs

    @retry_api()
    async def get_pair_data(self, pair_symbol: str, exchange: str) -> Optional[Dict]:
        """Получение данных по фьючерсной паре.

        Args:
            pair_symbol (str): Символ пары (например, 'BTC-USDT-SWAP').
            exchange (str): Название биржи ('okx').

        Returns:
            Optional[Dict]: Данные о паре или None при ошибке.
        """
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        async with aiohttp.ClientSession() as session:
            async with session.get(
                    f"{self.BASE_URL}/api/v5/market/tickers?instId={pair_symbol}",
                    ssl=ssl_context
            ) as resp:
                data = await resp.json()
                self.logger.debug(f"OKX API ticker response for {pair_symbol}: {data}")
                if data.get("code") != "0" or not data.get("data"):
                    self.logger.error(f"Ошибка API OKX для {pair_symbol}: {data.get('msg')}")
                    return None

                ticker = data["data"][0]
                try:
                    price = float(ticker["last"])
                    volume = float(ticker["vol24h"])
                    # Для open interest нужен отдельный запрос
                    async with session.get(
                            f"{self.BASE_URL}/api/v5/public/open-interest?instId={pair_symbol}",
                            ssl=ssl_context
                    ) as oi_resp:
                        oi_data = await oi_resp.json()
                        self.logger.debug(f"OKX API open interest for {pair_symbol}: {oi_data}")
                        if oi_data.get("code") != "0" or not oi_data.get("data"):
                            self.logger.error(f"Ошибка получения OI для {pair_symbol}")
                            return None
                        oi_contracts = float(oi_data["data"][0]["oi"])
                except (KeyError, ValueError) as e:
                    self.logger.error(f"Ошибка обработки данных для {pair_symbol}: {str(e)}")
                    return None

            # OKX не предоставляет funding rate в этом endpoint, используем заглушку
            funding_rate = 0.0
            base_symbol = pair_symbol.split("-")[0]

            return {
                "pair_symbol": pair_symbol,
                "base_symbol": base_symbol,
                "exchange": exchange,
                "open_interest_contracts": oi_contracts,
                "open_interest_usd": oi_contracts * price,
                "funding_rate": funding_rate,
                "volume_btc": 0.0,  # Требуется конвертация volume
                "volume_usd": volume * price,
                "price_usd": price,
                "market_cap_usd": 0.0,  # Не предоставляется
                "timestamp": datetime.now(timezone.utc)
            }