import csv
import logging
from typing import Optional

# Assuming MultipleScreenerItem is in util/request.py now
from util.request import MultipleScreenerItem
from util.exceptions import DataNotFoundError, InvalidInputError, ScrapingError

logger = logging.getLogger(__name__)

def list_params(filename: str) -> list:
    logger.debug(f"Listing parameters from file: {filename}")
    result = []
    if filename is None:
        logger.warning("Filename for list_params is None.")
        return []

    file_path = f'./src/screeners/{filename}'
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if 'name' in row:
                    result.append(row['name'].strip())
        if not result:
            logger.warning(f"No parameters found in {filename}.")
            # It's okay to return an empty list if the file is empty or has no 'name' column
        return result
    except FileNotFoundError as e:
        logger.error(f"Parameter file not found: {file_path} - {e}")
        raise DataNotFoundError(f"Parameter file '{filename}' not found.") from e
    except Exception as e:
        logger.critical(f"An unexpected error occurred while reading {file_path}: {e}")
        raise ScrapingError(f"An unexpected error occurred while reading parameter file '{filename}'.") from e


def checker_input(name: str, params: Optional[str]) -> bool:
    logger.debug(f"Checking input for {name} with value: {params}")
    mapping = {
        "cntry": "countries.csv",
        "sector": "sector.csv",
        "percentchange": "change_percent.csv",
        "volume": "volume.csv",
        "marketcap": "market_cap.csv",
        "price": "price.csv"
    }

    if params is None:
        return False

    filename = mapping.get(name)
    if not filename:
        logger.warning(f"No mapping found for checker_input name: {name}")
        return False

    file_path = f'./src/screeners/{filename}'
    try:
        data = {}
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if 'name' in row and 'value' in row:
                    data[row['name'].strip()] = row['value'].strip()
        
        is_valid = data.get(params) is not None
        if not is_valid:
            logger.warning(f"Invalid input '{params}' for parameter '{name}' (file: {filename}).")
        return is_valid
    except FileNotFoundError as e:
        logger.error(f"Checker input file not found: {file_path} - {e}")
        # For checker_input, we return False if the file isn't found, as it means the input can't be validated.
        return False
    except Exception as e:
        logger.critical(f"An unexpected error occurred while checking input for {name} from {file_path}: {e}")
        # For checker_input, we return False for unexpected errors during validation.
        return False


def screener_filter(items: MultipleScreenerItem) -> str:
    logger.debug(f"Filtering screener items: {items.model_dump_json()}")

    param_map = {
        "country": ("cntry", "countries.csv"),
        "industry": ("indtry", "industry.csv"),
        "sector": ("sector", "sector.csv"),
        "marketCap": ("marketcap", "market_cap.csv"),
        "changePercent": ("percentchange", "change_percent.csv"),
        "price": ("price", "price.csv"),
        "volume": ("volume", "volume.csv"),
        "pe": ("p_e", "pe.csv"),
        "forwardPE": ("fwd_pe", "forward_pe.csv"),
        "priceBook": ("pb", "price_book.csv"),
        "peg": ("peg", "peg.csv"),
        "earnings": ("earnings", "earnings.csv"),
        "profitMargin": ("profit_m", "profit_margin.csv"),
        "avgChg50D": ("davgchg50", "50D_avg_change.csv"),
        "returnOnAssets": ("roa", "return_on_assets.csv"),
        "epsThisYear": ("epsy", "eps_this_y.csv"),
        "float": ("flt", "float.csv"),
        "returnOnEquity": ("roe", "return_on_equity.csv"),
        "epsNextYear": ("epsny", "eps_next_y.csv"),
        "floatShort": ("fltsht", "float_short.csv"),
        "currentRatio": ("curr_r", "current_ratio.csv"),
        "epsPast5Year": ("epsp5y", "eps_past_5y.csv"),
        "sharesOutstanding": ("outstd", "shares_outstanding.csv"),
        "debtEquity": ("debteq", "debt_equity.csv"),
        "epsNext5Year": ("epsn5y", "eps_next_5y.csv"),
        "insiderOwn": ("insido", "insider_own.csv"),
        "dividendYield": ("dividend", "dividen_yield.csv"),
        "beta": ("beta", "beta.csv"),
        "grossMargin": ("gross_m", "gross_margin.csv"),
        "operatingMargin": ("oper_m", "operating_margin.csv"),
        "earnQuarterlyGrowth": ("ernqtrgrth", "earn_quarterly_growth.csv"),
        "avgChg200D": ("davgchg200", "200D_avg_change.csv"),
        "highChg52W": ("wkhchg52", "52W_high_change.csv"),
        "lowChg52W": ("wklchg52", "52W_low_change.csv"),
        "analystRecom": ("recomm", "analyst_recommendations.csv"),
    }

    query_params = []
    for item_key, (url_param_name, filename) in param_map.items():
        item_value = getattr(items, item_key, None)
        if item_value is not None:
            file_path = f'./src/screeners/{filename}'
            try:
                data_map = {}
                with open(file_path, 'r', encoding='utf-8') as file:
                    reader = csv.DictReader(file)
                    for row in reader:
                        if 'name' in row and 'value' in row:
                            data_map[row['name'].strip()] = row['value'].strip()
                
                mapped_value = data_map.get(item_value)
                if mapped_value is not None:
                    query_params.append(f"{url_param_name}={mapped_value}")
                else:
                    logger.warning(f"No mapped value found for '{item_value}' in '{filename}' for parameter '{item_key}'.")
            except FileNotFoundError:
                logger.error(f"Screener filter file not found: {file_path}")
                # Continue processing other parameters even if one file is missing
            except Exception as e:
                logger.critical(f"Error processing screener filter for '{item_key}' from '{filename}': {e}")
                # Continue processing other parameters

    final_params = "&".join(query_params)
    if not final_params:
        logger.warning("No valid screener filter parameters generated.")
        # Depending on requirements, might raise InvalidInputError here.
        # For now, returning an empty string which will result in a base screener URL.

    return final_params + "&t=overview&st=asc"