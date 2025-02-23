from curl_cffi.requests import AsyncSession
from curl_cffi.requests.exceptions import ProxyError, RequestException
from typing import Optional, Dict, List, Any, Union, AsyncGenerator, Type,Set
import asyncio
import json
import coloredlogs, logging
import msgspec
from msgspec import Struct, field

logger = logging.getLogger(__name__)
coloredlogs.install(level='DEBUG', logger=logger)
logging.basicConfig(
    format="%(asctime)-15s [%(levelname)s] %(funcName)s: %(message)s",
    level=logging.INFO)


class ProxyClientCFFI:
    def __init__(
        self,
        proxy_service_url: str,
        api_secret: str,
        impersonate: str ="chrome99_android",
        group_name: Optional[str] = None,
        priority_weights: Optional[Dict[str, int]] = None,
        # max_retries: int = 3
    ):
        self.proxy_service_url = proxy_service_url
        self.headers = {"x-secret": api_secret}
        self.impersonate = impersonate
        self.group_name = group_name
        self.priority_weights = priority_weights
        self.session: Optional[AsyncSession] = None
        # self.max_retries = max_retries
        self.params = self._build_params()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def start(self):
        self.session = AsyncSession(
            impersonate=self.impersonate,
            http_version=3,
            headers=self.headers
        )

    async def close(self):
        if self.session:
            await self.session.close()

    def _build_params(self) -> dict:
        params = {}
        if self.group_name:
            params["group_name"] = self.group_name
        if self.priority_weights:
            params["priority_weights"] = json.dumps(self.priority_weights)
        return params

    async def _get_proxy(self) -> dict:
        try:
            response = await self.session.get(
                self.proxy_service_url,
                params=self.params
            )
            response.raise_for_status()
            data = response.json()
            return {
                "http": data["url"],
                "https": data["url"]
            }
        except Exception as e:
            logging.error(f"Failed to get proxy: {e}")
            raise

    async def request(
        self, 
        method: str, 
        url: str, 
        response_type: Type[Struct] = None,
        strict: bool = True,
        timeout: float = 3.0,
        max_retries: int = 3,
        no_retry_statuses: Set[int] = None,
        no_retry_classes: Set[int] = None,
        base_delay: float = 0.0,
        exponential_factor: float = 2.0,
        **kwargs
    ) -> Union[Struct, dict, list, None]:
        # Инициализация декодера
        if response_type is not None:
            decoder = msgspec.json.Decoder(type=response_type, strict=strict)
        else:
            decoder = msgspec.json.Decoder()
        
        # Генерация статус-кодов для классов
        status_classes = {
            4: set(range(400, 500)),
            5: set(range(500, 562))
        }
        
        # Сбор исключенных статусов
        excluded_statuses = set()
        if no_retry_classes:
            for cls in no_retry_classes:
                excluded_statuses.update(status_classes.get(cls, set()))
        
        if no_retry_statuses:
            excluded_statuses.update(no_retry_statuses)
        
        # Основной цикл попыток
        for attempt in range(1, max_retries + 1):
            delay = base_delay * (exponential_factor ** (attempt - 1))
            try:
                proxy = await self._get_proxy()
                logging.info(f"Using proxy: {proxy} [Attempt {attempt}/{max_retries}]")
                
                response = await self.session.request(
                    method=method,
                    url=url,
                    proxies=proxy,
                    timeout=timeout,
                    **kwargs
                )
                
                # Проверка статус-кода
                if response.status_code in excluded_statuses:
                    logging.warning(f"Received excluded status {response.status_code}")
                    return decoder.decode(response.content) if response.content else None
                
                response.raise_for_status()
                
                return decoder.decode(response.content)
                
            except (ProxyError, RequestException) as e:
                # Обработка исключений с кодом статуса
                status_code = getattr(e, 'code', None)
                if status_code in excluded_statuses:
                    logging.warning(f"Excluded status {status_code} received")
                    return None
                
                logging.warning(f"Attempt {attempt} failed: {e}")
                if attempt < self.max_retries:
                    logging.info(f"Waiting {delay:.2f}s before next attempt")
                    await asyncio.sleep(delay)
                
            except msgspec.DecodeError as e:
                logging.error(f"Decoding error: {e}")
                return None
            except Exception as e:
                logging.error(f"Request failed: {e}")
                raise
                
        raise RequestException(f"Max retries ({self.max_retries}) exceeded")
