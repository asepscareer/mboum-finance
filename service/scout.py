import csv
import json


def list_country():
    countries = []
    with open('./src/screeners/countries.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            countries.append(row['name'])
    return countries


def list_stocks_by_country(param):
    stocks = []
    country = param.lower().replace(' ', '-')
    with open(f'./src/stocks/{country}.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            stocks.append({
                'ticker': row['ticker'],
                'company': row['company']
            })
    return json.dumps(stocks)

