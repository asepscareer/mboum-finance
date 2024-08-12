import json

import uvicorn
from fastapi import FastAPI

from service import Scraper

app = FastAPI()
service = Scraper()


@app.get("/")
def root():
    return {"message": "Hello Mboum Finance API"}


@app.get("/desc/{symbol}")
def desc(symbol: str):
    result = service.desc(symbol)
    return json.loads(result)


@app.get("/stats/{symbol}")
def stats(symbol: str):
    result = service.stats(symbol)
    return json.loads(result)


@app.get("/stock-news/{symbol}")
def stock_news(symbol: str):
    result = service.stock_news(symbol)
    return json.loads(result)


@app.get("/analyst-ratings/{symbol}")
def analyst_ratings(symbol: str):
    result = service.analyst_ratings(symbol)
    return json.loads(result)


@app.get("/insider-trades/{symbol}")
def insider_trades(symbol: str):
    result = service.insider_trades(symbol)
    return json.loads(result)


@app.get("/latest-news")
def latest_news():
    result = service.latest_news()
    return json.loads(result)


@app.get("/all-insider-trades")
def insider_trades_all():
    result = service.all_insider_trades()
    return json.loads(result)


@app.get("/multiple-screener")
def multiple_screener():
    result = service.multiple_screener()
    return json.loads(result)


@app.get("/screeners-scraper")
def screeners_scraper():
    result = service.screeners_scraper()
    return json.loads(result)


@app.get("/list-stock-country-scraper")
def list_stocks_country_scraper():
    result = service.list_stocks_country_scraper()
    return json.loads(result)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
