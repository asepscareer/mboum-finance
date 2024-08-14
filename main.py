import json

import uvicorn
from fastapi import FastAPI

from service import Scraper
from service import list_country, list_stocks_by_country
from util import response

app = FastAPI()
service = Scraper()


@app.get("/")
def root():
    return {"message": "Hello Mboum Finance API"}


@app.get("/desc/{symbol}")
def desc(symbol: str):
    result = service.desc(symbol)
    return response.failed() if result is None else response.success(json.loads(result))


@app.get("/stats/{symbol}")
def stats(symbol: str):
    result = service.stats(symbol)
    return response.failed() if result is None else response.success(json.loads(result))


@app.get("/stock-news/{symbol}")
def stock_news(symbol: str):
    result = service.stock_news(symbol)
    return response.failed() if result is None else response.success(json.loads(result))


@app.get("/analyst-ratings/{symbol}")
def analyst_ratings(symbol: str):
    result = service.analyst_ratings(symbol)
    return response.failed() if result is None else response.success(json.loads(result))


@app.get("/insider-trades/{symbol}")
def insider_trades(symbol: str):
    result = service.insider_trades(symbol)
    return response.failed() if result is None else response.success(json.loads(result))


@app.get("/latest-news")
def latest_news():
    result = service.latest_news()
    return response.failed() if result is None else response.success(json.loads(result))


@app.get("/all-insider-trades")
def insider_trades_all():
    result = service.all_insider_trades()
    return response.failed() if result is None else response.success(json.loads(result))


@app.get("/multiple-screener")
def multiple_screener():
    result = service.multiple_screener()
    return response.failed() if result is None else response.success(json.loads(result))


@app.get("/list-country")
def countries():
    result = list_country()
    return response.failed() if result is None else response.success(json.loads(result))


@app.get("/stocks-by-country/{country}")
def stocks_by_country(country: str):
    result = list_stocks_by_country(country)
    return response.failed() if result is None else response.success(json.loads(result))


@app.get("/oversold/{country}")
def oversold(country: str):
    result = service.oversold(country)
    return response.failed() if result is None else response.success(json.loads(result))


@app.get("/overbought-stocks/{country}")
def overbought_stocks(country: str):
    result = service.overbought_stocks(country)
    return response.failed() if result is None else response.success(json.loads(result))


@app.get("/upcoming-earnings/{country}")
def upcoming_earnings(country: str):
    result = service.upcoming_earnings(country)
    return response.failed() if result is None else response.success(json.loads(result))


# @app.get("/screeners-scraper")
def screeners_scraper():
    result = service.screeners_scraper()
    return response.failed() if result is None else response.success(json.loads(result))


# @app.get("/list-stock-country-scraper")
def list_stocks_country_scraper():
    result = service.list_stocks_country_scraper()
    return response.failed() if result is None else response.success(json.loads(result))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
