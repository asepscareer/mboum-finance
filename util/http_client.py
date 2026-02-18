import httpx
import logging
from httpx import RequestError, TimeoutException, HTTPStatusError

from util.exceptions import RequestFailedError, ScrapingError

logger = logging.getLogger(__name__)

class HttpClient:
    def __init__(self, base_url: str, headers: dict = None, timeout: int = 10):
        self.base_url = base_url
        self.headers = headers if headers is not None else {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0'
        }
        self.timeout = timeout
        self.session = httpx.AsyncClient(base_url=self.base_url, headers=self.headers, timeout=self.timeout)

    async def _request(self, method: str, path: str, **kwargs):
        url = f"{self.base_url}{path}" # httpx.AsyncClient handles base_url, so path should be relative
        logger.debug(f"Making {method} request to: {url}")
        try:
            response = await self.session.request(method, path, **kwargs) # Use path directly with AsyncClient
            response.raise_for_status()  # Raise HTTPStatusError for bad responses (4xx or 5xx)
            return response
        except TimeoutException as e:
            logger.error(f"Request to {url} timed out: {e}")
            raise RequestFailedError(f"Request to {url} timed out.") from e
        except HTTPStatusError as e:
            logger.error(f"HTTP error for {url}: {e.response.status_code} - {e.response.text}")
            raise RequestFailedError(f"HTTP error for {url}: {e.response.status_code} - {e.response.text}") from e
        except RequestError as e: # Catches ConnectionError, etc.
            logger.error(f"Request error for {url}: {e}")
            raise RequestFailedError(f"Request error for {url}: {e}.") from e
        except Exception as e:
            logger.critical(f"An unhandled exception occurred during HTTP request to {url}: {e}")
            raise ScrapingError(f"An unhandled error occurred during HTTP request to {url}.") from e

    async def get(self, path: str, **kwargs):
        return await self._request("GET", path, **kwargs)

    async def close(self):
        await self.session.aclose()
