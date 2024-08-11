import logging

import requests
from lxml import html
import json

from lxml.cssselect import CSSSelector


class Scraper:
    
    def __init__(self):
        self.url = "https://mboum.com"
        self.session = requests.Session()

    def stock_news(self, symbol):
        result = []
        try:
            response = self.session.get("{}/quote/{}".format(self.url, symbol))
            response.raise_for_status()
            tree = html.fromstring(response.content)
            rows = tree.cssselect('div.col-8 div.card-body table tr')
            print(len(rows))
            for row in rows:
                time = row.cssselect('td:first-child')[0].text
                title = row.cssselect('td a')[0].text
                link = row.cssselect('td a')[0].get('href')
                result.append({"time": time, "headline": title, "link": link})
        except Exception as e:
            logging.error("An unexpected error occurred: {}".format(e))
        finally:
            return json.dumps(result)

    def stats(self, symbol):
        def to_camel_case(text):
            components = text.split()
            return components[0].lower() + ''.join(x.title() for x in components[1:])

        response = self.session.get("{}/quote/{}".format(self.url, symbol))
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

    def desc(self, symbol):
        response = self.session.get("{}/quote/{}".format(self.url, symbol))
        tree = html.fromstring(response.content)

        company_name = tree.cssselect('.card-header')[0].text_content().replace('About ', '')
        description = tree.cssselect('.card-text')[0].text_content().strip()
        data = {'name': company_name, 'description': description}
        return json.dumps(data)

    def latest_news(self):
        response = self.session.get("{}/news".format(self.url))
        tree = html.fromstring(response.content)

        data = []
        for row in tree.cssselect('table tr'):
            time = row.cssselect('td')[0].text_content()
            headline_elem = row.cssselect('td a')[0]
            headline = headline_elem.text_content().strip()
            authors = headline_elem.getnext().text_content().strip()
            data.append({'time': time, 'headline': headline, 'authors': authors})
        return json.dumps(data)

    def analyst_ratings(self, symbol):
        result = []
        try:
            response = self.session.get("{}/quote/{}".format(self.url, symbol))
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
        except Exception as e:
            logging.error("An unexpected error occurred: {}".format(e))
        finally:
            return json.dumps(result)

    def insider_trades(self, symbol):
        result = []
        try:
            response = self.session.get("{}/quote/{}".format(self.url, symbol))
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
                    "SECForm4Link": sec_form_4_link.strip()
                })
        except Exception as e:
            logging.error("An unexpected error occurred: {}".format(e))
        finally:
            return json.dumps(result)
