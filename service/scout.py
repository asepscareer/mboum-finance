import csv
import json
import logging

from util import list_params


def list_country():
    countries = []
    try:
        with open('./src/screeners/countries.csv', 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                countries.append(row['name'])
        return countries
    except Exception as e:
        logging.error("An unexpected error occurred: {}".format(e))
        return None


def list_stocks_by_country(param):
    stocks = []
    try:
        country = param.lower().replace(' ', '-')
        with open(f'./src/stocks/{country}.csv', 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                stocks.append({
                    'ticker': row['ticker'],
                    'company': row['company']
                })
        return json.dumps(stocks)
    except Exception as e:
        logging.error("An unexpected error occurred: {}".format(e))
        return None


def multiple_screeners_params(params: str = None):
    mapping = {
        "countries": "countries.csv",
        "industry": "industry.csv",
        "sector": "sector.csv",
        "market-cap": "market_cap.csv",
        "change-percent": "change_percent.csv",
        "price": "price.csv",
        "volume": "volume.csv",
        "pe": "pe.csv",
        "forward-pe": "forward_pe.csv",
        "price-book": "price_book.csv",
        "peg": "peg.csv",
        "earnings": "earnings.csv",
        "profit-margin": "profit_margin.csv",
        "50D-avg-change": "50D_avg_change.csv",
        "return-on-assets": "return_on_assets.csv",
        "eps-this-year": "eps_this_y.csv",
        "float": "float.csv",
        "return-on-equity": "return_on_equity.csv",
        "eps-next-year": "eps_next_y.csv",
        "float-short": "float_short.csv",
        "current-ratio": "current_ratio.csv",
        "eps-past-5year": "eps_past_5y.csv",
        "shares-outstanding": "shares_outstanding.csv",
        "debt-equity": "debt_equity.csv",
        "eps-next-5year": "eps_next_5y.csv",
        "insider-own": "insider_own.csv",
        "dividen-yield": "dividen_yield.csv",
        "beta": "beta.csv",
        "gross-margin": "gross_margin.csv",
        "operating-margin": "operating_margin.csv",
        "earn-quarterly-growth": "earn_quarterly_growth.csv",
        "200D-avg-change": "200D_avg_change.csv",
        "52W-high-change": "52W_high_change.csv",
        "52W-low-change": "52W_low_change.csv",
        "analyst-recommendations": "analyst_recommendations.csv"
    }

    try:
        result = list_params(mapping.get(params))
        return result
    except Exception as e:
        logging.error("An unexpected error occurred: {}".format(e))
        return None

