import csv
import json
import logging

from util import list_params
from util.exceptions import DataNotFoundError, ScrapingError

logger = logging.getLogger(__name__)

def list_country():
    logger.info("Listing all countries from CSV.")
    countries = []
    try:
        with open('./src/screeners/countries.csv', 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if 'name' in row:
                    countries.append(row['name'].strip())
        if not countries:
            raise DataNotFoundError("No countries found in countries.csv.")
        return countries
    except FileNotFoundError as e:
        logger.error(f"countries.csv not found: {e}")
        raise ScrapingError("Country data file not found.") from e
    except Exception as e:
        logger.critical(f"An unexpected error occurred while listing countries: {e}")
        raise ScrapingError("An unexpected error occurred while listing countries.") from e


def list_stocks_by_country(param: str):
    logger.info(f"Listing stocks for country: {param}")
    stocks = []
    try:
        country_filename = param.lower().replace(' ', '-')
        file_path = f'./src/stocks/{country_filename}.csv'
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if 'ticker' in row and 'company' in row:
                    stocks.append({
                        'ticker': row['ticker'].strip(),
                        'company': row['company'].strip()
                    })
        if not stocks:
            raise DataNotFoundError(f"No stocks found for country '{param}' in {file_path}.")
        return json.dumps(stocks)
    except FileNotFoundError as e:
        logger.error(f"Stock data file for country '{param}' not found: {e}")
        raise DataNotFoundError(f"Stock data for country '{param}' not found.") from e
    except Exception as e:
        logger.critical(f"An unexpected error occurred while listing stocks for country '{param}': {e}")
        raise ScrapingError(f"An unexpected error occurred while listing stocks for country '{param}'.") from e


def multiple_screeners_params(): # No parameters
    logger.info("Fetching all multiple screener parameters.")
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

    all_params_data = {}
    for param_key, filename in mapping.items():
        try:
            options = list_params(filename)
            if options:
                all_params_data[param_key] = options
            else:
                logger.warning(f"No options found for screener parameter '{param_key}' from file '{filename}'.")
        except (DataNotFoundError, ScrapingError) as e:
            logger.error(f"Error fetching options for screener parameter '{param_key}': {e}")
            # Continue to next parameter even if one fails
        except Exception as e:
            logger.critical(f"An unexpected error occurred while fetching options for screener parameter '{param_key}': {e}")
            # Continue to next parameter

    if not all_params_data:
        raise DataNotFoundError("No screener parameters could be fetched.")
    
    return all_params_data

