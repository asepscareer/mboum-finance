import csv
import logging

import requests
from cssselect import GenericTranslator
from lxml import html
import json

from lxml.cssselect import CSSSelector


class Scraper:

    def __init__(self):
        self.url = "https://mboum.com"
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0'
        }

    def stock_news(self, symbol):
        result = []
        try:
            response = self.session.get("{}/quote/{}".format(self.url, symbol), headers=self.headers)
            response.raise_for_status()
            tree = html.fromstring(response.content)
            rows = tree.cssselect('div.col-8 div.card-body table tr')
            print(len(rows))
            for row in rows:
                time = row.cssselect('td:first-child')[0].text
                title = row.cssselect('td a')[0].text
                link = row.cssselect('td a')[0].get('href')
                result.append({"time": time, "headline": title, "link": link})
            return json.dumps(result)
        except Exception as e:
            logging.error("An unexpected error occurred: {}".format(e))
            return None

    def stats(self, symbol):
        def to_camel_case(text):
            components = text.split()
            return components[0].lower() + ''.join(x.title() for x in components[1:])

        try:
            response = self.session.get("{}/quote/{}".format(self.url, symbol), headers=self.headers)
            tree = html.fromstring(response.content)

            symbol = tree.xpath('//div[@class="text-center p-1"]/a[1]/b/text()')[0]
            company_name = tree.xpath('//div[@class="text-center p-1"]/a[2]/text()')[0]

            data = {
                'symbol': symbol,
                'name': company_name
            }

            rows = tree.xpath('//tr[@class="d-flex"]')
            for row in rows:
                columns = row.xpath('.//td')
                for i in range(0, len(columns), 2):
                    key = columns[i].text_content().strip()
                    value = columns[i + 1].xpath('.//b')[0].text_content().strip()
                    camel_case_key = to_camel_case(key)
                    data[camel_case_key] = value
            return json.dumps(data)
        except Exception as e:
            logging.error("An unexpected error occurred: {}".format(e))
            return None

    def desc(self, symbol):
        try:
            response = self.session.get("{}/quote/{}".format(self.url, symbol), headers=self.headers)
            tree = html.fromstring(response.content)

            company_name = tree.cssselect('.card-header')[0].text_content().replace('About ', '')
            description = tree.cssselect('.card-text')[0].text_content().strip()
            data = {'name': company_name, 'description': description}
            return json.dumps(data)
        except Exception as e:
            logging.error("An unexpected error occurred: {}".format(e))
            return None

    def latest_news(self):
        try:
            response = self.session.get("{}/news".format(self.url), headers=self.headers)
            tree = html.fromstring(response.content)

            data = []
            for row in tree.cssselect('table tr'):
                time = row.cssselect('td')[0].text_content()
                headline_elem = row.cssselect('td a')[0]
                headline = headline_elem.text_content().strip()
                authors = headline_elem.getnext().text_content().strip()
                data.append({'time': time, 'headline': headline, 'authors': authors})
            return json.dumps(data)
        except Exception as e:
            logging.error("An unexpected error occurred: {}".format(e))
            return None

    def analyst_ratings(self, symbol):
        result = []
        try:
            response = self.session.get("{}/quote/{}".format(self.url, symbol), headers=self.headers)
            tree = html.fromstring(response.content)
            analyst_ratings_table = tree.cssselect('div.card:contains("Analyst Ratings") table')[0]
            rows = analyst_ratings_table.cssselect('tr')
            for row in rows:
                columns = row.cssselect('td')
                if len(columns) == 3:
                    analyst = columns[0].text_content()
                    rating = columns[1].text_content()
                    date = columns[2].text_content()
                    result.append({"analyst": analyst, "rating": rating, "date": date})
            return json.dumps(result)
        except Exception as e:
            logging.error("An unexpected error occurred: {}".format(e))
            return None


    def insider_trades(self, symbol):
        result = []
        try:
            response = self.session.get("{}/quote/{}".format(self.url, symbol), headers=self.headers)
            tree = html.fromstring(response.content)

            row_selector = CSSSelector('table.table-sm tbody tr')

            for row in row_selector(tree):
                insider_trades_selector = CSSSelector('td:nth-child(1)')
                relationship_selector = CSSSelector('td:nth-child(2) span.home-insider-desktop')
                date_selector = CSSSelector('td:nth-child(3)')
                transactions_selector = CSSSelector('td:nth-child(4)')
                cost_selector = CSSSelector('td:nth-child(5)')
                shares_selector = CSSSelector('td:nth-child(6)')
                value_selector = CSSSelector('td:nth-child(7)')
                share_own_selector = CSSSelector('td:nth-child(8)')
                sec_form_4_selector = CSSSelector('td:nth-child(9) a')

                insider_trades = insider_trades_selector(row)[0].text_content() if insider_trades_selector(row) else ""
                relationship = relationship_selector(row)[0].text_content() if relationship_selector(row) else ""
                date = date_selector(row)[0].text_content() if date_selector(row) else ""
                transactions = transactions_selector(row)[0].text_content() if transactions_selector(row) else ""
                cost = cost_selector(row)[0].text_content() if cost_selector(row) else ""
                shares = shares_selector(row)[0].text_content() if shares_selector(row) else ""
                value = value_selector(row)[0].text_content() if value_selector(row) else ""
                share_own = share_own_selector(row)[0].text_content() if share_own_selector(row) else ""
                sec_form_4 = sec_form_4_selector(row)[0].text_content() if sec_form_4_selector(row) else ""
                sec_form_4_link = sec_form_4_selector(row)[0].get('href') if sec_form_4_selector(row) else ""

                result.append({
                    "insiderTrades": insider_trades.strip(),
                    "relationship": relationship.strip(),
                    "date": date.strip(),
                    "transactions": transactions.strip(),
                    "cost($)": cost.strip(),
                    "shares": shares.strip(),
                    "value($)": value.strip(),
                    "shareOwn": share_own.strip(),
                    "SECForm4": sec_form_4.strip(),
                    "SECForm4Link": sec_form_4_link
                })
            return json.dumps(result)
        except Exception as e:
            logging.error("An unexpected error occurred: {}".format(e))
            return None


    def screeners_scraper(self):
        result = []
        try:
            response = self.session.get("{}/screener".format(self.url), headers=self.headers)
            tree = html.fromstring(response.content)

            translator = GenericTranslator()
            xpath_expression = translator.css_to_xpath('select option')
            options = tree.xpath(xpath_expression)

            for option in options:
                result.append({
                    'name': option.text_content(),
                    'value': option.get('value')
                })
            with open('./src/output.csv', 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['name', 'value']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                writer.writeheader()
                for row in result:
                    writer.writerow(row)
            return json.dumps(result)
        except Exception as e:
            logging.error("An unexpected error occurred: {}".format(e))
            return None


    def all_insider_trades(self):
        result = []
        try:
            response = self.session.get("{}".format(self.url), headers=self.headers)
            tree = html.fromstring(response.content)

            translator = GenericTranslator()
            rows_xpath = translator.css_to_xpath('div.col-8.pb-4 table.table-sm tbody tr')
            rows = tree.xpath(rows_xpath)

            for row in rows:
                cols_xpath = translator.css_to_xpath('td')
                cols = row.xpath(cols_xpath)

                ticker = cols[0].xpath('.//a/text()')[0] if cols[0].xpath('.//a/text()') else ""
                insider = cols[1].xpath('.//span[@class="home-insider-desktop"]/text()')[0] if cols[1].xpath(
                    './/span[@class="home-insider-desktop"]/text()') else ""
                trade_type = cols[2].text_content()
                cost = cols[3].text_content()
                shares = cols[4].text_content()
                value = cols[5].text_content()
                shares_owned = cols[6].text_content()
                sec_form_4_date = cols[7].xpath('.//a/text()')[0] if cols[7].xpath('.//a/text()') else ""
                sec_form_4_link = cols[7].xpath('.//a/@href')[0] if cols[7].xpath('.//a/@href') else ""

                result.append({
                    'ticker': ticker.strip(),
                    'insider': insider.strip(),
                    'type': trade_type.strip(),
                    'cost($)': cost.strip(),
                    'shares': shares.strip(),
                    'value($)': value.strip(),
                    'shareOwn': shares_owned.strip(),
                    'SECForm4Date': sec_form_4_date.strip(),
                    'SECForm4Link': sec_form_4_link.strip(),
                })
            return json.dumps(result)
        except Exception as e:
            logging.error("An unexpected error occurred: {}".format(e))
        finally:
            logging.error("An unexpected error occurred: {}".format(e))
            return None

    def multiple_screener(self):
        result = []

        url = "{}/screener/1?cntry=united-states&percentchange=chg_up&price=vol_b_50_100&volume=vol_u_100&t=overview".format(
            self.url)
        try:
            response = self.session.get(url, headers=self.headers)
            tree = html.fromstring(response.content)
            result += self._screener(url)

            page_info_span = tree.cssselect('span:contains("Page")')
            if page_info_span:
                page_info_text = page_info_span[0].text_content().strip()
                total_pages = page_info_text.split()[-1]
                total_pages = int(total_pages)
            else:
                return 0

            for page in range(2, total_pages + 1):
                url_access = "{}/screener/{}?cntry=united-states&percentchange=chg_up&price=vol_b_50_100&volume=vol_u_100&t=overview".format(
                    self.url, page)
                result += self._screener(url_access)
            return json.dumps(result)
        except Exception as e:
            print(f"Error occurred during scraping: {e}")
            return None

    def _screener(self, url):
        result = []
        response = self.session.get(url, headers=self.headers)
        tree = html.fromstring(response.content)
        translator = GenericTranslator()

        rows_xpath = translator.css_to_xpath(
            'table.table.table-striped.table-hover.table-sm.table-bordered.analytic tbody tr')
        rows = tree.xpath(rows_xpath)
        for row in rows:
            cols_xpath = translator.css_to_xpath('td')
            cols = row.xpath(cols_xpath)

            ticker = cols[0].xpath('.//a/text()')[0] if cols[0].xpath('.//a/text()') else ""
            company = cols[1].text_content().strip()
            industry = cols[2].text_content().strip()
            sector = cols[3].text_content().strip()
            country = cols[4].text_content().strip()
            market_cap = cols[5].text_content().strip()
            price = cols[6].text_content().strip()
            change = cols[7].xpath('.//span/text()')[0] if cols[7].xpath('.//span/text()') else ""
            volume = cols[8].text_content().strip()

            result.append({
                'ticker': ticker.strip(),
                'company': company,
                'industry': industry,
                'sector': sector,
                'country': country,
                'marketCap': market_cap,
                'price': price,
                'change': change.strip(),
                'volume': volume
            })
        return result

    def list_stocks_country_scraper(self):
        try:
            with open('./src/screeners/countries.csv', 'r', newline='', encoding='utf-8') as files:
                reader = csv.DictReader(files)
                for r in reader:
                    country = r['value']
                    result = []
                    url = "{}/screener/1?cntry={}".format(self.url, country)
                    try:
                        page = self.session.get(url, headers=self.headers)
                        tree = html.fromstring(page.content)
                        result += self._screener_stocks(url)

                        page_info_span = tree.cssselect('span:contains("Page")')
                        if page_info_span:
                            page_info_text = page_info_span[0].text_content().strip()
                            total_pages = page_info_text.split()[-1]
                            total_pages = int(total_pages)
                        else:
                            total_pages = 0

                        if total_pages > 1:
                            for page in range(2, total_pages + 1):
                                url_access = "{}/screener/{}?cntry={}".format(self.url, page, country)
                                result += self._screener_stocks(url_access)

                        with open(f'./src/stocks/{country}.csv', 'w', newline='', encoding='utf-8') as csvfile:
                            fieldnames = ['ticker', 'company', 'country']
                            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                            writer.writeheader()
                            for row in result:
                                writer.writerow(row)
                    except Exception as e:
                        print(f"Error occurred during scraping: {e}")
                        return None
            return json.dumps({"status": "success"})
        except Exception as e:
            print(f"Error occurred during scraping: {e}")
            return None

    def _screener_stocks(self, url):
        result = []
        response = self.session.get(url, headers=self.headers)
        tree = html.fromstring(response.content)
        translator = GenericTranslator()

        rows_xpath = translator.css_to_xpath(
            'table.table.table-striped.table-hover.table-sm.table-bordered.analytic tbody tr')
        rows = tree.xpath(rows_xpath)
        for row in rows:
            cols_xpath = translator.css_to_xpath('td')
            cols = row.xpath(cols_xpath)

            ticker = cols[0].xpath('.//a/text()')[0] if cols[0].xpath('.//a/text()') else ""
            company = cols[1].text_content().strip()
            industry = cols[2].text_content().strip()
            sector = cols[3].text_content().strip()
            country = cols[4].text_content().strip()

            result.append({
                'ticker': ticker.strip(),
                'company': company,
                'industry': industry,
                'sector': sector,
                'country': country,
            })
        return result

    def oversold(self, country):
        result = []
        value_country = ""
        try:
            with open('./src/screeners/countries.csv', 'r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if row['name'].strip().lower() == country.lower():
                        value_country = row['value']
                        break
            url = "{}/screener?cntry={}&dividend=div_0_1&volume=vol_o_50&wklchg52=52wklchg_up_0_5&recomm=recomm_1_3&st=desc".format(
                self.url, value_country)
            result += self._screener_stocks(url)
        except Exception as e:
            logging.error("An unexpected error occurred: {}".format(e))
        finally:
            return json.dumps(result)

    def overbought_stocks(self, country):
        result = []
        value_country = ""
        try:
            with open('./src/screeners/countries.csv', 'r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if row['name'].strip().lower() == country.lower():
                        value_country = row['value']
                        break
            url = "{}/screener?cntry={}&dividend=div_u&volume=vol_o_50&wkhchg52=52wkhchg_down_0_5&recomm=recomm_1_35&st=desc".format(
                self.url, value_country)
            result += self._screener_stocks(url)
        except Exception as e:
            logging.error("An unexpected error occurred: {}".format(e))
        finally:
            return json.dumps(result)

    def upcoming_earnings(self, country):
        result = []
        value_country = ""
        try:
            with open('./src/screeners/countries.csv', 'r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if row['name'].strip().lower() == country.lower():
                        value_country = row['value']
                        break
            url = "{}/screener?cntry={}&earnings=earnings_tw&price=price_o_10&volume=vol_o_500&t=overview&st=asc".format(
                self.url, value_country)
            result += self._screener_stocks(url)
        except Exception as e:
            logging.error("An unexpected error occurred: {}".format(e))
        finally:
            return json.dumps(result)
