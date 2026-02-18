import json
import logging
import uvicorn
import uuid
import time
import redis
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, status

from service import Scraper
from util import success, failed
from util.exceptions import ScrapingError, DataNotFoundError, RequestFailedError, InvalidInputError
from config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

REDIS_URL = "redis://default:wXewwNFntlQAhZJbAeszQUbPcEfoFywL@metro.proxy.rlwy.net:50331/1"
DEFAULT_CACHE_TTL = 300
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    logger.info("Starting up application...")
    app.state.redis_client = None
    try:
        app.state.redis_client = redis.asyncio.from_url(REDIS_URL) # Use redis.asyncio
        await app.state.redis_client.ping()
        logger.info("Successfully connected to Redis.")
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Could not connect to Redis: {e}")
        app.state.redis_client = None
    except Exception as e:
        logger.error(f"An unexpected error occurred during Redis connection: {e}")
        app.state.redis_client = None
    
    app.state.scraper_service = Scraper(redis_client=app.state.redis_client)
    
    yield
    
    # Shutdown logic
    logger.info("Shutting down application...")
    if app.state.redis_client:
        await app.state.redis_client.close()
        logger.info("Redis connection closed.")
    if app.state.scraper_service.http_client:
        await app.state.scraper_service.http_client.close() # Await the async close() method
        logger.info("HTTPX client closed.")

app = FastAPI(lifespan=lifespan)

@app.middleware("http")
async def add_trace_id_middleware(request: Request, call_next):
    trace_id = str(uuid.uuid4())
    request.state.trace_id = trace_id
    
    start_time = time.time()
    try:
        response = await call_next(request)
    except Exception as e:
        process_time = time.time() - start_time
        logger.error(f"Unhandled exception during request: {e}", extra={'trace_id': trace_id})
        response = await generic_exception_handler(request, e)
    
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    response.headers["X-Trace-ID"] = trace_id
    logger.info(f"Request completed: {request.method} {request.url.path} - {response.status_code}", 
                extra={'trace_id': trace_id})
    return response

@app.exception_handler(ScrapingError)
async def scraping_error_handler(request: Request, exc: ScrapingError):
    trace_id = request.state.trace_id
    logger.error(f"ScrapingError occurred: {exc}", extra={'trace_id': trace_id})
    return failed(
        message="An internal server error occurred during data scraping. Please contact the API provider.",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        trace_id=trace_id
    )

@app.exception_handler(DataNotFoundError)
async def data_not_found_error_handler(request: Request, exc: DataNotFoundError):
    trace_id = request.state.trace_id
    logger.warning(f"DataNotFoundError occurred: {exc}", extra={'trace_id': trace_id})
    return failed(
        message=str(exc),
        status_code=status.HTTP_404_NOT_FOUND,
        trace_id=trace_id
    )

@app.exception_handler(RequestFailedError)
async def request_failed_error_handler(request: Request, exc: RequestFailedError):
    trace_id = request.state.trace_id
    logger.error(f"RequestFailedError occurred: {exc}", extra={'trace_id': trace_id})
    return failed(
        message="Failed to retrieve data from external service. Please try again later or contact the API provider.",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        trace_id=trace_id
    )

@app.exception_handler(InvalidInputError)
async def invalid_input_error_handler(request: Request, exc: InvalidInputError):
    trace_id = request.state.trace_id
    logger.warning(f"InvalidInputError occurred: {exc}", extra={'trace_id': trace_id})
    return failed(
        message=str(exc),
        status_code=status.HTTP_400_BAD_REQUEST,
        trace_id=trace_id
    )

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    trace_id = request.state.trace_id
    logger.error(f"HTTPException occurred: {exc.detail}", extra={'trace_id': trace_id})
    return failed(
        message=exc.detail,
        status_code=exc.status_code,
        trace_id=trace_id
    )

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    trace_id = request.state.trace_id
    logger.exception(f"An unhandled exception occurred: {exc}", extra={'trace_id': trace_id})
    return failed(
        message="An unexpected internal server error occurred. Please contact the API provider.",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        trace_id=trace_id
    )

@app.get("/", operation_id="get-root")
async def root():
    return {"message": "Hello Mboum Finance API"}

@app.get("/health", operation_id="get-health")
async def health():
    return {"status": "healthy"}

@app.get("/news/latest", tags=["News"], operation_id="get-latest-news")
async def latest_news(request: Request):
    result = await app.state.scraper_service.latest_news(trace_id=request.state.trace_id)
    return success(json.loads(result))

@app.get("/stock/overview/{symbol}", tags=["Stocks"], operation_id="get-stock-overview")
async def overview(request: Request, symbol: str = "AAPL"):
    result = await app.state.scraper_service.overview(symbol, trace_id=request.state.trace_id)
    return success(json.loads(result))

@app.get("/stock/price/{symbol}", tags=["Stocks"], operation_id="get-stock-price")
async def stock_price(request: Request, symbol: str = "AAPL"):
    result = await app.state.scraper_service.get_price_data(symbol, trace_id=request.state.trace_id)
    return success(json.loads(result))

@app.get("/stock/related-news/{symbol}", tags=["Stocks"], operation_id="get-stock-related-news")
async def related_news(request: Request, symbol: str = "AAPL"):
    result = await app.state.scraper_service.related_news(symbol, trace_id=request.state.trace_id)
    return success(json.loads(result))

@app.get("/stock/financials/{symbol}", tags=["Stocks"], operation_id="get-stock-financials")
async def financials(request: Request, symbol: str = "AAPL"):
    result = await app.state.scraper_service.financials(symbol, trace_id=request.state.trace_id)
    return success(json.loads(result))


@app.get("/stock/insider-trades-filter", tags=["Stocks"], operation_id="get-stock-insider-trades")
async def insider_trades(request: Request, page: int = 1, transaction_type: str = None, transaction_value: str = None, politician: str = None):
    base_path = "/stocks/insider-trades"
    
    params = {
        "page": page,
        "t": transaction_type,
        "v": transaction_value,
        "p": politician,
    }
    
    active_params = [
        f"{k}={v}" for k, v in params.items() 
        if v is not None and not (k == "page" and v == 1)
    ]
    
    url_path = f"{base_path}?{'&'.join(active_params)}" if active_params else base_path

    result = await app.state.scraper_service.all_insider_trades(
        page=page, url_path=url_path, type ="filtered",
        trace_id=request.state.trace_id
    )

    return success(json.loads(result))


@app.get("/stock/all-insider-trades", tags=["Stocks"], operation_id="get-all-insider-trades")
async def insider_trades_all(request: Request, page: int = 1):
    if page == 1:
        url_path = "/stocks/insider-trades"
    else:
        url_path = f"/stocks/insider-trades?page={page}"
    result = await app.state.scraper_service.all_insider_trades(page=page, url_path=url_path, trace_id=request.state.trace_id)
    return success(json.loads(result))

@app.get("/stock/ipos", tags=["Stocks"], operation_id="get-ipos")
async def ipos(request: Request):
    result = await app.state.scraper_service.ipos(trace_id=request.state.trace_id)
    return success(json.loads(result))

@app.get("/stock/upcoming-dividends", tags=["Stocks"], operation_id="get-upcoming-dividends")
async def upcoming_dividends(request: Request):
    result = await app.state.scraper_service.upcoming_dividends(trace_id=request.state.trace_id)
    return success(json.loads(result))

@app.get("/stocks/earnings", tags=["Stocks"], operation_id="get-earnings")
async def get_earnings(request: Request, d: str, page: int = 1):
    result = await app.state.scraper_service.earnings(d, page, trace_id=request.state.trace_id)
    return success(json.loads(result))

@app.get("/stocks/market-movers", tags=["Stocks"], operation_id="get-market-movers")
async def get_market_movers(request: Request, page: int = 1):
    result = await app.state.scraper_service.market_movers(page, trace_id=request.state.trace_id)
    return success(json.loads(result))


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)
