import json

import uvicorn
from fastapi import FastAPI

from service import Scraper
from service import list_country, list_stocks_by_country, multiple_screeners_params
from util import MultipleScreenerItem, success, failed, screener_filter, checker_input

app = FastAPI()
service = Scraper()


@app.get("/")
def root():
    return {"message": "Hello Mboum Finance API"}

@app.get("/latest-news", tags=["News"])
def latest_news():
    result = service.latest_news()
    return failed() if result is None else success(json.loads(result))

@app.get("/stock/overview/{symbol}", tags=["Stocks"])
def overview(symbol: str):
    result = service.overview(symbol)
    return failed() if result is None else success(json.loads(result))

@app.get("/stock/related-news/{symbol}", tags=["Stocks"])
def related_news(symbol: str):
    result = service.related_news(symbol)
    return failed() if result is None else success(json.loads(result))

@app.get("/stock/financials/{symbol}", tags=["Stocks"])
def financials(symbol: str):
    result = service.financials(symbol)
    return failed() if result is None else success(json.loads(result))

"""
BELUM DIPERBAIKI
"""

@app.get("/desc/{symbol}", tags=["Stocks"])
def desc(symbol: str):
    result = service.desc(symbol)
    return failed() if result is None else success(json.loads(result))


@app.get("/stats/{symbol}", tags=["Stocks"])
def stats(symbol: str):
    result = service.stats(symbol)
    return failed() if result is None else success(json.loads(result))


@app.get("/analyst-ratings/{symbol}", tags=["Stocks"])
def analyst_ratings(symbol: str):
    result = service.analyst_ratings(symbol)
    return failed() if result is None else success(json.loads(result))


@app.get("/insider-trades/{symbol}", tags=["Stocks"])
def insider_trades(symbol: str):
    result = service.insider_trades(symbol)
    return failed() if result is None else success(json.loads(result))


@app.get("/all-insider-trades")
def insider_trades_all():
    result = service.all_insider_trades()
    return failed() if result is None else success(json.loads(result))


@app.get("/list-country")
def countries():
    result = list_country()
    return failed() if result is None else success(result)


@app.get("/multiple-screener-params")
def multiple_screener_params(key: str):
    result = multiple_screeners_params(key)
    return failed() if result is None else success(result)


@app.get("/stocks-by-country/{country}", tags=["Screener"])
def stocks_by_country(country: str):
    result = list_stocks_by_country(country)
    return failed() if result is None else success(json.loads(result))


@app.get("/oversold/{country}", tags=["Screener"])
def oversold(country: str):
    result = service.oversold(country)
    return failed() if result is None else success(json.loads(result))


@app.get("/overbought-stocks/{country}", tags=["Screener"])
def overbought_stocks(country: str):
    result = service.overbought_stocks(country)
    return failed() if result is None else success(json.loads(result))


@app.get("/upcoming-earnings/{country}", tags=["Screener"])
def upcoming_earnings(country: str):
    result = service.upcoming_earnings(country)
    return failed() if result is None else success(json.loads(result))


@app.post("/multiple-screener")
def multiple_screener(items: MultipleScreenerItem):
    result = service.multiple_screener(items)
    return failed() if result is None else success(json.loads(result))


# @app.get("/screeners-scraper")
def screeners_scraper():
    result = service.screeners_scraper()
    return failed() if result is None else success(json.loads(result))


# @app.get("/list-stock-country-scraper")
def list_stocks_country_scraper():
    result = service.list_stocks_country_scraper()
    return failed() if result is None else success(json.loads(result))


if __name__ == "__main__":
    # data = MultipleScreenerItem(country="Ok")
    # print(screenerFilter(data))
    # print(checkerInput('cntry', 'United Kingdom'))
    # print(multiple_screeners_params('asep'))
    uvicorn.run(app, host="0.0.0.0", port=8000)
