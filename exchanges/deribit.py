import aiohttp
import ssl
import certifi
import logging
from typing import List, Dict, Optional
from utils.retry import retry_api


class DeribitClient:
    """Клиент для работы с Deribit API."""

    BASE_URL = "https://www.deribit.com"
    SUPPORTED_CURRENCIES = ["BTC", "ETH", "SOL", "USDC"]

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
            for currency in self.SUPPORTED_CURRENCIES:
                try:
                    async with session.get(
                            f"{self.BASE_URL}/api/v2/public/get_instruments?currency={currency}",
                            ssl=ssl_context
                    ) as resp:
                        data = await resp.json()
                        self.logger.debug(f"Deribit API response for currency={currency}: {data}")
                        if "result" not in data:
                            self.logger.error(f"Ошибка API Deribit для currency={currency}: отсутствует ключ 'result'")
                            continue
                        for item in data["result"]:
                            instrument_type = item.get("instrument_type", "unknown")
                            kind = item.get("kind", "unknown")
                            self.logger.debug(
                                f"Instrument for {currency}: name={item.get('instrument_name')}, "
                                f"type={instrument_type}, kind={kind}"
                            )
                            if kind in ["perpetual", "future"]:
                                base_symbol = item["base_currency"]
                                instrument_name = item["instrument_name"]
                                # Исключаем кросс-пары вроде ETH_BTC
                                if "_" not in instrument_name or instrument_name.endswith("-PERPETUAL"):
                                    pairs.append({
                                        "symbol": instrument_name,
                                        "base_symbol": base_symbol
                                    })
                except aiohttp.ClientError as e:
                    self.logger.error(f"Ошибка запроса к API Deribit для currency={currency}: {str(e)}")
                    continue
        self.logger.info(f"Получено {len(pairs)} фьючерсных пар с Deribit")
        return pairs

    @retry_api()
    async def get_pair_data(self, pair_symbol: str, exchange: str) -> Optional[Dict]:
        """Получение данных по фьючерсной паре.

        Args:
            pair_symbol (str): Символ пары (например, 'BTC-PERPETUAL').
            exchange (str): Название биржи ('deribit').

        Returns:
            Optional[Dict]: Данные о паре или None при ошибке.

        Raises:
            aiohttp.ClientError: Ошибка при запросе к API.
        """
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        async with aiohttp.ClientSession() as session:
            async with session.get(
                    f"{self.BASE_URL}/api/v2/public/get_book_summary_by_instrument?instrument_name={pair_symbol}",
                    ssl=ssl_context
            ) as resp:
                data = await resp.json()
                self.logger.debug(f"Deribit API response for pair={pair_symbol}: {data}")
                if "result" not in data or not data["result"]:
                    self.logger.error(f"Ошибка API Deribit для {pair_symbol}: некорректный ответ")
                    return None

                summary = data["result"][0]
                try:
                    volume_usd = float(summary["volume_usd"])
                    price = float(summary["last"])
                    oi_contracts = float(summary["open_interest"])
                except (KeyError, ValueError) as e:
                    self.logger.error(f"Ошибка обработки данных для {pair_symbol}: {str(e)}")
                    return None

            # Deribit не предоставляет фандинговые ставки для фьючерсов
            funding_rate = 0.0

            # Извлечение base_symbol с проверкой формата
            try:
                base_symbol = pair_symbol.split("-")[0]
            except IndexError:
                self.logger.error(f"Некорректный формат символа {pair_symbol}")
                return None

            return {
                "pair_symbol": pair_symbol,
                "base_symbol": base_symbol,
                "exchange": exchange,
                "open_interest_contracts": oi_contracts,
                "open_interest_usd": oi_contracts * price,
                "funding_rate": funding_rate,
                "volume_usd": volume_usd
            }