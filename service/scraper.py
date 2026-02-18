import re
import csv
import logging
import json
import functools
import hashlib
import redis

from cssselect import GenericTranslator
from lxml import html

from util import MultipleScreenerItem, screener_filter, checker_input
from util.http_client import HttpClient
from util.exceptions import ScrapingError, DataNotFoundError, RequestFailedError, InvalidInputError

logger = logging.getLogger(__name__)

DEFAULT_CACHE_TTL = 180 # 5 minutes

def to_snake_case(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1\2', s1).lower().replace(' ', '_').replace('.', '').replace('%', 'percent').replace('(', '').replace(')', '')

def cache_result(ttl: int = DEFAULT_CACHE_TTL): # Removed redis_client from arguments
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs): # 'self' is the Scraper instance
            if not self.redis_client: # Access redis_client from the instance
                logger.warning(f"Redis not connected. Skipping cache for {func.__name__}.")
                return await func(self, *args, **kwargs)

            # Generate a cache key based on function name and arguments
            cache_args = []
            for arg in args:
                if isinstance(arg, MultipleScreenerItem):
                    cache_args.append(arg.model_dump_json())
                else:
                    cache_args.append(str(arg))
            
            for k, v in kwargs.items():
                if isinstance(v, MultipleScreenerItem):
                    cache_args.append(f"{k}={v.model_dump_json()}")
                else:
                    cache_args.append(f"{k}={v}")

            cache_key_str = f"{func.__name__}:{hashlib.md5(json.dumps(cache_args, sort_keys=True).encode('utf-8')).hexdigest()}"
            
            try:
                cached_data = await self.redis_client.get(cache_key_str) # Use self.redis_client
                if cached_data:
                    logger.info(f"Cache hit for {func.__name__} with key: {cache_key_str}")
                    return cached_data.decode('utf-8')
            except Exception as e:
                logger.error(f"Error retrieving from Redis cache for key {cache_key_str}: {e}")

            logger.info(f"Cache miss for {func.__name__} with key: {cache_key_str}. Fetching data.")
            result = await func(self, *args, **kwargs)
            
            if result is not None:
                try:
                    await self.redis_client.setex(cache_key_str, ttl, result) # Use self.redis_client
                    logger.debug(f"Cached result for {func.__name__} with key: {cache_key_str}, TTL: {ttl}s")
                except Exception as e:
                    logger.error(f"Error setting to Redis cache for key {cache_key_str}: {e}")
            return result
        return wrapper
    return decorator

class Scraper:
    def __init__(self, redis_client: redis.Redis = None): # Accept redis_client as dependency
        self.base_url = "https://mboum.com"
        self.http_client = HttpClient(base_url=self.base_url)
        self.redis_client = redis_client # Assign the passed client

    def _parse_html(self, content: str):
        try:
            return html.fromstring(content)
        except Exception as e:
            logger.error(f"Failed to parse HTML content: {e}")
            raise ScrapingError("Failed to parse HTML content.") from e

    @cache_result(ttl=60)
    async def latest_news(self, trace_id: str = "N/A"):
        logger.info("Fetching latest news...", extra={'trace_id': trace_id})
        try:
            response = await self.http_client.get("/news")
            tree = self._parse_html(response.content)

            rows = tree.cssselect("div.card div.row")
            if not rows:
                logger.warning("No news items found.", extra={'trace_id': trace_id})
                raise DataNotFoundError("No news items found.")

            data = []
            for row in rows:
                try:
                    img_elem = row.cssselect("img")
                    img_url = img_elem[0].get("src") if img_elem else None

                    time_elem = row.cssselect("p.mb-1 small")
                    time = time_elem[0].text_content().strip() if time_elem else None

                    title_elem = row.cssselect("h5 a")
                    headline = title_elem[0].text_content().strip() if title_elem else None
                    link = title_elem[0].get("href") if title_elem else None

                    desc_elem = row.cssselect("p.text-clamp")
                    description = desc_elem[0].text_content().strip() if desc_elem else None

                    ticker_elems = row.cssselect("a.badge")
                    tickers = [el.text_content().strip() for el in ticker_elems]

                    if headline and link: # Only add if essential data is present
                        data.append({
                            "time": time,
                            "headline": headline,
                            "link": link,
                            "description": description,
                            "related_tickers": tickers,
                            "image": img_url
                        })
                except IndexError:
                    logger.debug("Skipping malformed news item due to missing elements.", extra={'trace_id': trace_id})
                    continue # Skip malformed rows
                except Exception as e:
                    logger.warning(f"Error processing a news item: {e}", extra={'trace_id': trace_id})
                    continue

            if not data:
                raise DataNotFoundError("No valid news data could be extracted.")
            return json.dumps(data, indent=2)
        except (RequestFailedError, ScrapingError, DataNotFoundError) as e:
            logger.error(f"Error in latest_news: {e}", extra={'trace_id': trace_id})
            raise
        except Exception as e:
            logger.critical(f"An unexpected error occurred in latest_news: {e}", extra={'trace_id': trace_id})
            raise ScrapingError("An unexpected error occurred while fetching latest news.") from e
        
    @cache_result(ttl=60)
    async def overview(self, symbol: str, trace_id: str = "N/A"):
        logger.info(f"Fetching overview and description for symbol: {symbol}", extra={'trace_id': trace_id})
        def arrange_key(text: str):
            return text.replace(' ', '_').lower()

        try:
            # --- Fetch data from /quotes/{symbol} (for description and company details) ---
            response_desc = await self.http_client.get(f"/quotes/{symbol}")
            tree_desc = self._parse_html(response_desc.content)

            description_elem = tree_desc.cssselect('#summaryPreview p')
            description = description_elem[0].text_content().strip() if description_elem else None

            details_data = {}
            details_rows = tree_desc.cssselect('div.row.row-cols-1.row-cols-md-2.mt-3.px-3.text-muted.small > div')
            for row in details_rows:
                strong_elem = row.cssselect('strong')
                if strong_elem:
                    label = strong_elem[0].text_content().strip().replace(':', '').lower().replace(' ', '_')
                    if label == 'website':
                        value_elem = row.cssselect('a')
                        value = value_elem[0].get('href').strip() if value_elem else None
                    elif label == 'address':
                        address_div_elem = row.cssselect('div') # Use last-child for address
                        value = address_div_elem[1].text_content().strip() if address_div_elem else None
                    else:
                        value = row.text_content().replace(strong_elem[0].text_content(), '').strip()
                    
                    if value:
                        details_data[label] = value

            response_overview = await self.http_client.get(f"/quotes/{symbol}?v=overview")
            tree_overview = self._parse_html(response_overview.content)

            company_name_elem = tree_overview.cssselect("h1.page-title")
            full_company_name = company_name_elem[0].text_content().strip() if company_name_elem else None
            
            company_name = None
            symbol_extracted = None
            if full_company_name:
                match = re.match(r"^(.*)\s+\(([^)]+)\)$", full_company_name)
                if match:
                    company_name = match.group(1).strip()
                    symbol_extracted = match.group(2).strip()
                else:
                    company_name = full_company_name # Fallback if format doesn't match
                    symbol_extracted = symbol # Use the requested symbol as a fallback

            if not company_name:
                raise DataNotFoundError(f"Could not find essential overview data for {symbol}.")

            overview_data = {}
            table_rows = tree_overview.cssselect("table.quote-table tr")
            for row in table_rows:
                label_elem = row.cssselect("td.quote-label")
                value_elem = row.cssselect("td.quote-val")
                if label_elem and value_elem:
                    label = arrange_key(label_elem[0].text_content().strip())
                    value = value_elem[0].text_content().strip()
                    overview_data[label] = value

            # --- Combine all data ---
            final_result = {
                "company_name": company_name,
                "symbol": symbol_extracted,
                "description": description,
                "industry": details_data.get('industry'),
                "sector": details_data.get('sector'),
                "phone": details_data.get('phone'),
                "website": details_data.get('website'),
                "address": details_data.get('address'),
                "summary": overview_data
            }
            return json.dumps(final_result, indent=2)
        except (RequestFailedError, ScrapingError, DataNotFoundError) as e:
            logger.error(f"Error in overview for {symbol}: {e}", extra={'trace_id': trace_id})
            raise
        except Exception as e:
            logger.critical(f"An unexpected error occurred in overview for {symbol}: {e}", extra={'trace_id': trace_id})
            raise ScrapingError(f"An unexpected error occurred while fetching overview for {symbol}.") from e
        
    @cache_result(ttl=60)
    async def get_price_data(self, symbol: str, trace_id: str = "N/A"):
        logger.info(f"Fetching price data for symbol: {symbol}", extra={'trace_id': trace_id})
        try:
            response = await self.http_client.get(f"/quotes/{symbol}?v=overview")
            tree = self._parse_html(response.content)

            company_name_elem = tree.cssselect("h1.page-title")
            full_company_name = company_name_elem[0].text_content().strip() if company_name_elem else None
            
            company_name_extracted = None
            if full_company_name:
                match = re.match(r"^(.*)\s+\(([^)]+)\)$", full_company_name)
                if match:
                    company_name_extracted = match.group(1).strip()
                else:
                    company_name_extracted = "N/A"

            price_elem = tree.cssselect("span.quote-price")
            price = price_elem[0].text_content().strip() if price_elem else None
            
            price_change_elem = tree.cssselect("span.quote-price.text-success, span.quote-price.text-danger")
            full_price_change = price_change_elem[0].text_content().strip() if price_change_elem else None

            price_change_abs = None
            price_change_percent = None
            if full_price_change:
                match = re.match(r"^(-?\d+\.?\d*)\s+\(([^)]+)\)$", full_price_change)
                if match:
                    price_change_abs = match.group(1).strip()
                    price_change_percent = match.group(2).strip()
                else:
                    price_change_abs = full_price_change # Fallback if format doesn't match

            if not company_name_extracted or not price:
                raise DataNotFoundError(f"Could not find essential price data for {symbol}.")

            result = {
                "symbol": symbol,
                "company_name": company_name_extracted,
                "price": price,
                "price_change": price_change_abs,
                "price_change_%": price_change_percent,
            }
            return json.dumps(result, indent=2)
        except (RequestFailedError, ScrapingError, DataNotFoundError) as e:
            logger.error(f"Error in get_price_data for {symbol}: {e}", extra={'trace_id': trace_id})
            raise
        except Exception as e:
            logger.critical(f"An unexpected error occurred in get_price_data for {symbol}: {e}", extra={'trace_id': trace_id})
            raise ScrapingError(f"An unexpected error occurred while fetching price data for {symbol}.") from e
        
    @cache_result(ttl=60)
    async def related_news(self, symbol: str, trace_id: str = "N/A"):
        logger.info(f"Fetching related news for symbol: {symbol}", extra={'trace_id': trace_id})
        try:
            response = await self.http_client.get(f"/quotes/{symbol}?v=overview")
            tree = self._parse_html(response.content)

            news_items = []
            rows = tree.cssselect("div.card-body.pt-3 div.row")

            if not rows:
                logger.warning(f"No related news found for {symbol}.", extra={'trace_id': trace_id})
                raise DataNotFoundError(f"No related news found for {symbol}.")

            for row in rows:
                try:
                    image_elem = row.cssselect("img")
                    image_url = image_elem[0].get("src").strip() if image_elem else None

                    date_elem = row.cssselect("p.mb-1 small")
                    date_text = date_elem[0].text_content().strip() if date_elem else None

                    title_elem = row.cssselect("h5.mb-2 a")
                    title = title_elem[0].text_content().strip() if title_elem else None
                    link = title_elem[0].get("href").strip() if title_elem else None

                    if title and link: # Only add if essential data is present
                        news_items.append({
                            "title": title,
                            "url": link,
                            "image": image_url,
                            "published": date_text
                        })
                except Exception as e:
                    logger.warning(f"Skipping one related news item for {symbol} due to error: {e}", extra={'trace_id': trace_id})
                    continue

            if not news_items:
                raise DataNotFoundError(f"No valid related news data could be extracted for {symbol}.")
            return json.dumps(news_items, indent=2)

        except (RequestFailedError, ScrapingError, DataNotFoundError) as e:
            logger.error(f"Error in related_news for {symbol}: {e}", extra={'trace_id': trace_id})
            raise
        except Exception as e:
            logger.critical(f"An unexpected error occurred in related_news for {symbol}: {e}", extra={'trace_id': trace_id})
            raise ScrapingError(f"An unexpected error occurred while fetching related news for {symbol}.") from e


    @cache_result(ttl=60)
    async def financials(self, symbol: str, trace_id: str = "N/A"):
        logger.info(f"Fetching financials for symbol: {symbol}", extra={'trace_id': trace_id})
        try:
            response = await self.http_client.get(f"/quotes/{symbol}?v=financial")
            tree = self._parse_html(response.content)
            data = {}

            def get_table_data(title_text):
                section = tree.xpath(f"//h5[contains(text(), '{title_text}')]")
                if not section:
                    logger.debug(f"Section '{title_text}' not found for {symbol}.", extra={'trace_id': trace_id})
                    return {}
                table = None
                for sibling in section[0].itersiblings():
                    if sibling.tag == 'table':
                        table = sibling
                        break
                if table is None: 
                    logger.debug(f"Table for section '{title_text}' not found for {symbol}.", extra={'trace_id': trace_id})
                    return {}
                
                rows = table.xpath(".//tr")
                result = {}
                for row in rows:
                    cols = row.xpath(".//td")
                    if len(cols) == 2:
                        key = to_snake_case(cols[0].text_content().strip()) # Apply to_snake_case here
                        value = cols[1].text_content().strip()
                        result[key] = value
                return result

            data['total_valuation'] = get_table_data("Total Valuation")
            data['stock_price_statistics'] = get_table_data("Stock Price Statistics")
            data['ownership_structure'] = get_table_data("Ownership and Share Structure")
            data['financial_performance'] = get_table_data("Financial Metrics and Performance")
            data['valuation_metrics'] = get_table_data("Valuation Metrics")

            if not any(data.values()):
                raise DataNotFoundError(f"No financial data could be extracted for {symbol}.")

            return json.dumps(data, indent=2)
        except (RequestFailedError, ScrapingError, DataNotFoundError) as e:
            logger.error(f"Error in financials for {symbol}: {e}", extra={'trace_id': trace_id})
            raise
        except Exception as e:
            logger.critical(f"An unexpected error occurred in financials for {symbol}: {e}", extra={'trace_id': trace_id})
            raise ScrapingError(f"An unexpected error occurred while fetching financials for {symbol}.") from e

    @cache_result(ttl=60)
    async def all_insider_trades(self, page: int, url_path: str = None, type: str = None, trace_id: str = "N/A"):
        logger.info(f"Fetching insider trades for page {url_path}.", extra={'trace_id': trace_id})
        current_page_trades = []
        try:
            # Fetch the requested page
            response = await self.http_client.get(url_path)
            tree = self._parse_html(response.content)

            # Extract total pages
            total_pages = 1
            pagination_links = tree.cssselect('ul.pagination li a.page-link')
            if pagination_links:
                for link in reversed(pagination_links):
                    if link.get('rel') != 'next':
                        try:
                            page_num = int(link.text_content().strip())
                            if page_num > total_pages:
                                total_pages = page_num
                            break
                        except ValueError:
                            continue

            # Scrape data from the current page
            translator = GenericTranslator()
            rows_xpath = translator.css_to_xpath('table.table-sm.table-striped.table-hover.mboum-tables tbody tr')
            rows = tree.xpath(rows_xpath)

            if not rows:
                logger.warning(f"No insider trades found on page {page}.", extra={'trace_id': trace_id})
                response_message = "Data not found or not available for the current page."
            else:
                response_message = "Insider trades retrieved successfully."
            
            for row in rows:
                cols_xpath = translator.css_to_xpath('td')
                cols = row.xpath(cols_xpath)

                if len(cols) >= 9:
                    ticker = cols[0].xpath('.//a/text()')[0].strip() if cols[0].xpath('.//a/text()') else ""
                    owner = cols[1].text_content().strip()
                    relationship = cols[2].text_content().strip()
                    date = cols[3].text_content().strip()
                    transaction = cols[4].text_content().strip()
                    cost = cols[5].text_content().strip()
                    shares = cols[6].text_content().strip()
                    value = cols[7].text_content().strip()
                    shares_total = cols[8].text_content().strip()

                    current_page_trades.append({
                        'ticker': ticker,
                        'owner': owner,
                        'relationship': relationship,
                        'date': date,
                        'transaction': transaction,
                        'cost': cost,
                        'shares': shares,
                        'value($)': value,
                        'shares_total': shares_total,
                    })
            
            response_data = {
                "trades": current_page_trades,
                "current_page": page,
                "total_pages": total_pages,
                "message": response_message
            }
            return json.dumps(response_data, indent=2)
        except (RequestFailedError, ScrapingError, DataNotFoundError) as e:
            logger.error(f"Error in all_insider_trades for page {page}: {e}", extra={'trace_id': trace_id})
            raise
        except Exception as e:
            logger.critical(f"An unexpected error occurred in all_insider_trades for page {page}: {e}", extra={'trace_id': trace_id})
            raise ScrapingError("An unexpected error occurred while fetching all insider trades.") from e

    @cache_result(ttl=60)
    async def ipos(self, trace_id: str = "N/A"):
        logger.info("Fetching upcoming IPOs...", extra={'trace_id': trace_id})
        try:
            response = await self.http_client.get("/ipos")
            tree = self._parse_html(response.content)

            ipos_data = []
            # The table has id="stocks-form" and then a table inside it
            rows = tree.cssselect('form#stocks-form table tbody tr')

            if not rows:
                logger.warning("No upcoming IPOs found.", extra={'trace_id': trace_id})
                raise DataNotFoundError("No upcoming IPOs found.")

            for row in rows:
                cols = row.cssselect('td')
                if len(cols) >= 7:
                    symbol = cols[0].cssselect('a')[0].text_content().strip() if cols[0].cssselect('a') else ""
                    name = cols[1].text_content().strip()
                    expected_date = cols[2].text_content().strip()
                    ipo_price = cols[3].text_content().strip()
                    shares_offered = cols[4].text_content().strip()
                    offer_amount = cols[5].text_content().strip()
                    exchange = cols[6].text_content().strip()

                    ipos_data.append({
                        "symbol": symbol,
                        "name": name,
                        "expected_date": expected_date,
                        "ipo_price": ipo_price,
                        "shares_offered": shares_offered,
                        "offer_amount": offer_amount,
                        "exchange": exchange,
                    })
            
            if not ipos_data:
                raise DataNotFoundError("No valid upcoming IPOs data could be extracted.")

            return json.dumps(ipos_data, indent=2)
        except (RequestFailedError, ScrapingError, DataNotFoundError) as e:
            logger.error(f"Error in upcoming_ipos: {e}", extra={'trace_id': trace_id})
            raise
        except Exception as e:
            logger.critical(f"An unexpected error occurred in upcoming_ipos: {e}", extra={'trace_id': trace_id})
            raise ScrapingError("An unexpected error occurred while fetching upcoming IPOs.") from e
        
    @cache_result(ttl=60)
    async def upcoming_dividends(self, trace_id: str = "N/A"):
        logger.info("Fetching upcoming dividends...", extra={'trace_id': trace_id})
        try:
            response = await self.http_client.get("/stocks/dividends")
            tree = self._parse_html(response.content)

            dividends_data = []
            # The table is inside a form with id="stocks-form"
            rows = tree.cssselect('form#stocks-form table.mboum-tables tbody tr')

            if not rows:
                logger.warning("No upcoming dividends found.", extra={'trace_id': trace_id})
                raise DataNotFoundError("No upcoming dividends found.")

            for row in rows:
                cols = row.cssselect('td')
                if len(cols) >= 8:
                    symbol = cols[0].cssselect('a')[0].text_content().strip() if cols[0].cssselect('a') else ""
                    name = cols[1].text_content().strip()
                    last = cols[2].text_content().strip()
                    change = cols[3].text_content().strip()
                    dividend = cols[4].text_content().strip()
                    yield_percent = cols[5].text_content().strip()
                    ex_div_date = cols[6].text_content().strip()
                    payable_date = cols[7].text_content().strip()

                    dividends_data.append({
                        "symbol": symbol,
                        "name": name,
                        "last": last,
                        "change": change,
                        "dividend": dividend,
                        "yield": yield_percent,
                        "ex_div_date": ex_div_date,
                        "payable_date": payable_date,
                    })
            
            if not dividends_data:
                raise DataNotFoundError("No valid upcoming dividends data could be extracted.")

            return json.dumps(dividends_data, indent=2)
        except (RequestFailedError, ScrapingError, DataNotFoundError) as e:
            logger.error(f"Error in upcoming_dividends: {e}", extra={'trace_id': trace_id})
            raise
        except Exception as e:
            logger.critical(f"An unexpected error occurred in upcoming_dividends: {e}", extra={'trace_id': trace_id})
            raise ScrapingError("An unexpected error occurred while fetching upcoming dividends.") from e
        

    def _screener(self, tree: html.HtmlElement):
        # This is a helper for parsing screener tables, not making requests
        result = []
        translator = GenericTranslator()

        rows_xpath = translator.css_to_xpath(
            'table.table.table-striped.table-hover.table-sm.table-bordered.analytic tbody tr')
        rows = tree.xpath(rows_xpath)
        
        if not rows:
            logger.debug("No rows found in screener table.")
            return []

        for row in rows:
            cols_xpath = translator.css_to_xpath('td')
            cols = row.xpath(cols_xpath)

            if len(cols) >= 9: # Ensure enough columns exist
                ticker = cols[0].xpath('.//a/text()')[0].strip() if cols[0].xpath('.//a/text()') else ""
                company = cols[1].text_content().strip()
                industry = cols[2].text_content().strip()
                sector = cols[3].text_content().strip()
                country = cols[4].text_content().strip()
                market_cap = cols[5].text_content().strip()
                price = cols[6].text_content().strip()
                change = cols[7].xpath('.//span/text()')[0].strip() if cols[7].xpath('.//span/text()') else ""
                volume = cols[8].text_content().strip()

                result.append({
                    'ticker': ticker,
                    'company': company,
                    'industry': industry,
                    'sector': sector,
                    'country': country,
                    'marketCap': market_cap,
                    'price': price,
                    'change': change,
                    'volume': volume
                })
        return result

    async def list_stocks_country_scraper(self, trace_id: str = "N/A"):
        logger.info("Scraping stocks by country.", extra={'trace_id': trace_id})
        try:
            # This part reads from a local CSV, which might be a side effect.
            # Consider if this should be part of the API response or a separate utility.
            # For now, keeping it as is to maintain original functionality.
            with open('./src/screeners/countries.csv', 'r', newline='', encoding='utf-8') as files:
                reader = csv.DictReader(files)
                for r in reader:
                    country_name = r['name']
                    country_value = r['value']
                    logger.info(f"Scraping stocks for country: {country_name} (value: {country_value})", extra={'trace_id': trace_id})
                    
                    country_stocks = []
                    initial_url = f"/screener/1?cntry={country_value}"
                    
                    try:
                        response = await self.http_client.get(initial_url)
                        tree = self._parse_html(response.content)
                        country_stocks.extend(self._screener_stocks(tree))

                        page_info_span = tree.cssselect('span:contains("Page")')
                        total_pages = 1
                        if page_info_span:
                            page_info_text = page_info_span[0].text_content().strip()
                            try:
                                total_pages = int(page_info_text.split()[-1])
                            except (ValueError, IndexError):
                                logger.warning(f"Could not parse total pages for {country_name}. Assuming 1 page.", extra={'trace_id': trace_id})

                        if total_pages > 1:
                            for page in range(2, total_pages + 1):
                                url_access = f"/screener/{page}?cntry={country_value}"
                                logger.debug(f"Fetching stocks for {country_name}, page {page}/{total_pages}", extra={'trace_id': trace_id})
                                page_response = self.http_client.get(url_access)
                                page_tree = self._parse_html(page_response.content)
                                country_stocks.extend(self._screener_stocks(page_tree))

                        if not country_stocks:
                            logger.warning(f"No stocks found for country {country_name}.", extra={'trace_id': trace_id})
                            # Continue to next country even if no stocks found for current one

                        # Write to country-specific CSV
                        output_filename = f'./src/stocks/{country_name.lower().replace(" ", "-")}.csv'
                        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
                            fieldnames = ['ticker', 'company', 'industry', 'sector', 'country']
                            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                            writer.writeheader()
                            writer.writerows(country_stocks)
                        logger.info(f"Stocks for {country_name} saved to {output_filename}", extra={'trace_id': trace_id})

                    except (RequestFailedError, ScrapingError) as e:
                        logger.error(f"Error scraping stocks for {country_name}: {e}", extra={'trace_id': trace_id})
                        # Continue to next country even if one fails
                    except IOError as e:
                        logger.critical(f"File I/O error while writing stocks for {country_name} to CSV: {e}", extra={'trace_id': trace_id})
                        # Continue to next country even if one fails
                    except Exception as e:
                        logger.critical(f"An unexpected error occurred while scraping stocks for {country_name}: {e}", extra={'trace_id': trace_id})
                        # Continue to next country even if one fails
            
            return json.dumps({"status": "success", "message": "All countries scraped successfully (check logs for individual country errors)."})
        except IOError as e:
            logger.critical(f"File I/O error while reading countries.csv: {e}", extra={'trace_id': trace_id})
            raise ScrapingError(f"Failed to read countries list: {e}") from e
        except Exception as e:
            logger.critical(f"An unexpected error occurred in get_countries: {e}", extra={'trace_id': trace_id})
            raise ScrapingError(f"An unexpected error occurred while fetching countries.") from e

    @cache_result(ttl=60)
    async def earnings(self, date_filter: str, page: int = 1, trace_id: str = "N/A"):
        logger.info(f"Fetching earnings for date filter: {date_filter}, page: {page}", extra={'trace_id': trace_id})
        
        valid_date_filters = ["yesterday", "today", "tomorrow", "last_week", "next_week", "last_month", "next_month"]
        if date_filter not in valid_date_filters:
            raise InvalidInputError(f"Invalid date filter: {date_filter}. Must be one of {', '.join(valid_date_filters)}.")

        try:
            url = f"/stocks/earnings?d={date_filter}&page={page}"
            response = await self.http_client.get(url)
            tree = self._parse_html(response.content)

            earnings_data = []
            rows = tree.cssselect('table.mboum-tables tbody tr')

            if not rows:
                logger.warning(f"No earnings data found for {date_filter} on page {page}.", extra={'trace_id': trace_id})
                raise DataNotFoundError(f"No earnings data found for {date_filter} on page {page}.")

            for row in rows:
                cols = row.cssselect('td')
                if len(cols) >= 12: # Ensure enough columns exist
                    symbol = cols[0].cssselect('a')[0].text_content().strip() if cols[0].cssselect('a') else ""
                    name = cols[1].text_content().strip()
                    stock_price = cols[2].text_content().strip()
                    change = cols[3].text_content().strip()
                    percent_change = cols[4].text_content().strip()
                    date = cols[5].text_content().strip()
                    time = cols[6].text_content().strip()
                    avg_earnings_est = cols[7].text_content().strip()
                    reported = cols[8].text_content().strip()
                    surprise = cols[9].text_content().strip()
                    surprise_percent = cols[10].text_content().strip()
                    volume = cols[11].text_content().strip()

                    earnings_data.append({
                        "symbol": symbol,
                        "name": name,
                        "stock_price": stock_price,
                        "change": change,
                        "percent_change": percent_change,
                        "date": date,
                        "time": time,
                        "avg_earnings_est": avg_earnings_est,
                        "reported": reported,
                        "surprise": surprise,
                        "surprise_percent": surprise_percent,
                        "volume": volume,
                    })
            
            # Pagination extraction
            total_pages = 1
            next_page_url = None
            
            pagination_links = tree.cssselect('ul.pagination li a.page-link')
            if pagination_links:
                # Find total pages from the last non-next link
                for link in reversed(pagination_links):
                    if link.get('rel') != 'next':
                        try:
                            page_num = int(link.text_content().strip())
                            if page_num > total_pages:
                                total_pages = page_num
                            break
                        except ValueError:
                            continue
                
                # Find the next page URL
                next_link = tree.cssselect('ul.pagination li a[rel="next"]')
                if next_link:
                    next_page_url = next_link[0].get('href')

            result = {
                "earnings": earnings_data,
                "current_page": page,
                "total_pages": total_pages,
                "next_page_url": next_page_url.replace("https://mboum.com", "").replace("d=", "param=") if next_page_url else None
            }

            if not earnings_data:
                raise DataNotFoundError(f"No valid earnings data could be extracted for {date_filter} on page {page}.")

            return json.dumps(result, indent=2)
        except (RequestFailedError, ScrapingError, DataNotFoundError, InvalidInputError) as e:
            logger.error(f"Error in earnings for {date_filter}, page {page}: {e}", extra={'trace_id': trace_id})
            raise
        except Exception as e:
            logger.critical(f"An unexpected error occurred in earnings for {date_filter}, page {page}: {e}", extra={'trace_id': trace_id})
            raise ScrapingError(f"An unexpected error occurred while fetching earnings for {date_filter}.")

    @cache_result(ttl=60)
    async def market_movers(self, page: int = 1, trace_id: str = "N/A"):
        logger.info(f"Fetching market movers for page: {page}", extra={'trace_id': trace_id})

        try:
            url = f"/stocks/movers?d=up&page={page}"
            response = await self.http_client.get(url)
            tree = self._parse_html(response.content)

            movers_data = []
            rows = tree.cssselect('table.mboum-tables tbody tr')

            for row in rows:
                cols = row.cssselect('td')
                if len(cols) >= 21: # Ensure enough columns exist based on the provided HTML
                    symbol = cols[0].cssselect('a')[0].text_content().strip() if cols[0].cssselect('a') else ""
                    name = cols[1].text_content().strip()
                    last = cols[2].text_content().strip()
                    change = cols[3].text_content().strip()
                    percent_change = cols[4].text_content().strip()
                    market_cap_k = cols[5].text_content().strip()
                    ent_value_k = cols[6].text_content().strip()
                    shares_out_k = cols[7].text_content().strip()
                    sales_a = cols[8].text_content().strip()
                    net_income_a = cols[9].text_content().strip()
                    sales_q = cols[10].text_content().strip()
                    net_income_q = cols[11].text_content().strip()
                    beta = cols[12].text_content().strip()
                    percent_insider = cols[13].text_content().strip()
                    percent_institutional = cols[14].text_content().strip()
                    float_k = cols[15].text_content().strip()
                    percent_float = cols[16].text_content().strip()
                    five_y_rev_percent = cols[17].text_content().strip()
                    ebit_a = cols[18].text_content().strip()
                    ebitda_a = cols[19].text_content().strip()
                    last_trade = cols[20].text_content().strip()

                    movers_data.append({
                        "symbol": symbol,
                        "name": name,
                        "last": last,
                        "change": change,
                        "percent_change": percent_change,
                        "market_cap_k": market_cap_k,
                        "ent_value_k": ent_value_k,
                        "shares_out_k": shares_out_k,
                        "sales_a": sales_a,
                        "net_income_a": net_income_a,
                        "sales_q": sales_q,
                        "net_income_q": net_income_q,
                        "beta": beta,
                        "percent_insider": percent_insider,
                        "percent_institutional": percent_institutional,
                        "float_k": float_k,
                        "percent_float": percent_float,
                        "five_y_rev_percent": five_y_rev_percent,
                        "ebit_a": ebit_a,
                        "ebitda_a": ebitda_a,
                        "last_trade": last_trade,
                    })
            
            # Pagination extraction (same logic as earnings)
            total_pages = 1
            next_page_url = None
            
            pagination_links = tree.cssselect('ul.pagination li a.page-link')
            if pagination_links:
                for link in reversed(pagination_links):
                    if link.get('rel') != 'next':
                        try:
                            page_num = int(link.text_content().strip())
                            if page_num > total_pages:
                                total_pages = page_num
                            break
                        except ValueError:
                            continue
                
                next_link = tree.cssselect('ul.pagination li a[rel="next"]')
                if next_link:
                    next_page_url = next_link[0].get('href')

            result = {
                "movers": movers_data,
                "current_page": page,
                "total_pages": total_pages,
                "next_page_url": next_page_url.replace("https://mboum.com/stocks/movers?d=up&", "/stocks/market-movers?") if next_page_url else None
            }

            if not movers_data:
                raise DataNotFoundError(f"No valid market movers data could be extracted for page {page}.")

            return json.dumps(result, indent=2)
        except (RequestFailedError, ScrapingError, DataNotFoundError, InvalidInputError) as e:
            logger.error(f"Error in market_movers for page {page}: {e}", extra={'trace_id': trace_id})
            raise
        except Exception as e:
            logger.critical(f"An unexpected error occurred in market_movers for page {page}: {e}", extra={'trace_id': trace_id})
            raise ScrapingError(f"An unexpected error occurred while fetching market movers for page {page}.")

    def _screener_stocks(self, tree: html.HtmlElement):
        # This is a helper for parsing screener tables, not making requests
        result = []
        translator = GenericTranslator()

        rows_xpath = translator.css_to_xpath(
            'table.table.table-striped.table-hover.table-sm.table-bordered.analytic tbody tr')
        rows = tree.xpath(rows_xpath)
        
        if not rows:
            logger.debug("No rows found in screener stocks table.")
            return []

        for row in rows:
            cols_xpath = translator.css_to_xpath('td')
            cols = row.xpath(cols_xpath)

            if len(cols) >= 5: # Ensure enough columns exist
                ticker = cols[0].xpath('.//a/text()')[0].strip() if cols[0].xpath('.//a/text()') else ""
                company = cols[1].text_content().strip()
                industry = cols[2].text_content().strip()
                sector = cols[3].text_content().strip()
                country = cols[4].text_content().strip()

                result.append({
                    'ticker': ticker,
                    'company': company,
                    'industry': industry,
                    'sector': sector,
                    'country': country,
                })
        return result

    @cache_result(ttl=60)
    async def oversold(self, country: str, trace_id: str = "N/A"):
        logger.info(f"Fetching oversold stocks for country: {country}", extra={'trace_id': trace_id})
        value_country = ""
        try:
            with open('./src/screeners/countries.csv', 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                found = False
                for row in reader:
                    if row['name'].strip().lower() == country.lower():
                        value_country = row['value']
                        found = True
                        break
                if not found:
                    raise DataNotFoundError(f"Country '{country}' not found in countries list.")
            
            url_params = f"cntry={value_country}&dividend=div_0_1&volume=vol_o_50&wklchg52=52wklchg_up_0_5&recomm=recomm_1_3&st=desc"
            response = await self.http_client.get(f"/screener?{url_params}")
            tree = self._parse_html(response.content)
            result = self._screener_stocks(tree)
            
            if not result:
                raise DataNotFoundError(f"No oversold stocks found for {country}.")
            return json.dumps(result)
        except (RequestFailedError, ScrapingError, DataNotFoundError) as e:
            logger.error(f"Error in oversold for {country}: {e}", extra={'trace_id': trace_id})
            raise
        except IOError as e:
            logger.critical(f"File I/O error while reading countries.csv for oversold: {e}", extra={'trace_id': trace_id})
            raise ScrapingError(f"Failed to read countries list for oversold: {e}") from e
        except Exception as e:
            logger.critical(f"An unexpected error occurred in oversold for {country}: {e}", extra={'trace_id': trace_id})
            raise ScrapingError(f"An unexpected error occurred while fetching oversold stocks for {country}.") from e

    @cache_result(ttl=60)
    async def overbought_stocks(self, country: str, trace_id: str = "N/A"):
        logger.info(f"Fetching overbought stocks for country: {country}", extra={'trace_id': trace_id})
        value_country = ""
        try:
            with open('./src/screeners/countries.csv', 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                found = False
                for row in reader:
                    if row['name'].strip().lower() == country.lower():
                        value_country = row['value']
                        found = True
                        break
                if not found:
                    raise DataNotFoundError(f"Country '{country}' not found in countries list.")
            
            url_params = f"cntry={value_country}&dividend=div_u&volume=vol_o_50&wkhchg52=52wkhchg_down_0_5&recomm=recomm_1_35&st=desc"
            response = await self.http_client.get(f"/screener?{url_params}")
            tree = self._parse_html(response.content)
            result = self._screener_stocks(tree)
            
            if not result:
                raise DataNotFoundError(f"No overbought stocks found for {country}.")
            return json.dumps(result)
        except (RequestFailedError, ScrapingError, DataNotFoundError) as e:
            logger.error(f"Error in overbought_stocks for {country}: {e}", extra={'trace_id': trace_id})
            raise
        except IOError as e:
            logger.critical(f"File I/O error while reading countries.csv for overbought_stocks: {e}", extra={'trace_id': trace_id})
            raise ScrapingError(f"Failed to read countries list for overbought_stocks: {e}") from e
        except Exception as e:
            logger.critical(f"An unexpected error occurred in overbought_stocks for {country}: {e}", extra={'trace_id': trace_id})
            raise ScrapingError(f"An unexpected error occurred while fetching overbought stocks for {country}.") from e

    @cache_result(ttl=60)
    async def upcoming_earnings(self, country: str, trace_id: str = "N/A"):
        logger.info(f"Fetching upcoming earnings for country: {country}", extra={'trace_id': trace_id})
        value_country = ""
        try:
            with open('./src/screeners/countries.csv', 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                found = False
                for row in reader:
                    if row['name'].strip().lower() == country.lower():
                        value_country = row['value']
                        found = True
                        break
                if not found:
                    raise DataNotFoundError(f"Country '{country}' not found in countries list.")
            
            url_params = f"cntry={value_country}&earnings=earnings_tw&price=price_o_10&volume=vol_o_500&t=overview&st=asc"
            response = await self.http_client.get(f"/screener?{url_params}")
            tree = self._parse_html(response.content)
            result = self._screener_stocks(tree)
            
            if not result:
                raise DataNotFoundError(f"No upcoming earnings found for {country}.")
            return json.dumps(result)
        except (RequestFailedError, ScrapingError, DataNotFoundError) as e:
            logger.error(f"Error in upcoming_earnings for {country}: {e}", extra={'trace_id': trace_id})
            raise
        except IOError as e:
            logger.critical(f"File I/O error while reading countries.csv for upcoming_earnings: {e}", extra={'trace_id': trace_id})
            raise ScrapingError(f"Failed to read countries list for upcoming_earnings: {e}") from e
        except Exception as e:
            logger.critical(f"An unexpected error occurred in upcoming_earnings for {country}: {e}", extra={'trace_id': trace_id})
            raise ScrapingError(f"An unexpected error occurred while fetching upcoming earnings for {country}.") from e


    @cache_result(ttl=3600)
    async def screeners_scraper(self, trace_id: str = "N/A"):
        logger.info("Scraping screeners data.", extra={'trace_id': trace_id})
        result = []
        try:
            response = await self.http_client.get("/screener")
            tree = self._parse_html(response.content)

            translator = GenericTranslator()
            xpath_expression = translator.css_to_xpath('select option')
            options = tree.xpath(xpath_expression)

            if not options:
                raise DataNotFoundError("No screener options found.")

            for option in options:
                result.append({
                    'name': option.text_content().strip(),
                    'value': option.get('value')
                })
            
            if not result:
                raise DataNotFoundError("No valid screener options could be extracted.")

            # This part writes to a local CSV, which might be a side effect.
            # Consider if this should be part of the API response or a separate utility.
            # For now, keeping it as is to maintain original functionality.
            with open('./src/output.csv', 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['name', 'value']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(result)
            
            logger.info("Screener data scraped and saved to src/output.csv", extra={'trace_id': trace_id})
            return json.dumps(result)
        except (RequestFailedError, ScrapingError, DataNotFoundError) as e:
            logger.error(f"Error in screeners_scraper: {e}", extra={'trace_id': trace_id})
            raise
        except IOError as e:
            logger.critical(f"File I/O error while writing screeners to CSV: {e}", extra={'trace_id': trace_id})
            raise ScrapingError(f"Failed to write screener data to CSV: {e}") from e
        except Exception as e:
            logger.critical(f"An unexpected error occurred in screeners_scraper: {e}", extra={'trace_id': trace_id})
            raise ScrapingError("An unexpected error occurred while scraping screeners.") from e
