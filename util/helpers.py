import csv

from util import MultipleScreenerItem


def checkerInput(name: str, params: str) -> bool:
    mapping = {
        "cntry": "countries.csv",
        "sector": "sector.csv",
        "percentchange": "change_percent.csv",
        "volume": "volume.csv",
        "marketcap": "market_cap.csv",
        "price": "price.csv"
    }

    if params is not None:
        data = {}
        with open(f'./src/screeners/{mapping.get(name)}', 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data[row['name'].strip()] = row['value'].strip()
        if data.get(params) is not None:
            return True
    return False


def screenerFilter(items: MultipleScreenerItem):
    def add_value(filename, params: str):
        _value = None
        if params is not None:
            data = {}
            with open(f'./src/screeners/{filename}', 'r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    data[row['name'].strip().strip()] = row['value'].strip()
                _value = data.get(params)
        return '' if _value is None else _value

    default = "&t=overview&st=asc"
    country_ = ["cntry", "countries.csv"]
    industry_ = ["indtry", "industry.csv"]
    sector = ["sector", "sector.csv"]
    market_cap_ = ["marketcap", "market_cap.csv"]
    percent_change_ = ["percentchange", "change_percent.csv"]
    price_ = ["price", "price.csv"]
    volume_ = ["volume", "volume.csv"]
    pe_ = ["p_e", "pe.csv"]
    fwd_pe_ = ["fwd_pe", "forward_pe.csv"]
    pb_ = ["pb", "price_book.csv"]
    peg_ = ["peg", "peg.csv"]
    earnings_ = ["earnings", "earnings.csv"]
    profit_m_ = ["profit_m", "profit_margin.csv"]
    davgchg50_ = ["davgchg50", "50D_avg_change.csv"]
    roa_ = ["roa", "return_on_assets.csv"]
    epsy_ = ["epsy", "eps_this_y.csv"]
    flt_ = ["flt", "float.csv"]
    roe_ = ["roe", "return_on_equity.csv"]
    epsny_ = ["epsny", "eps_next_y.csv"]
    fltsht_ = ["fltsht", "float_short.csv"]
    curr_r_ = ["curr_r", "current_ratio.csv"]
    epsp5y_ = ["epsp5y", "eps_past_5y.csv"]
    outstd_ = ["outstd", "shares_outstanding.csv"]
    debteq_ = ["debteq", "debt_equity.csv"]
    epsn5y_ = ["epsn5y", "eps_next_5y.csv"]
    insido_ = ["insido", "insider_own.csv"]
    dividend_ = ["dividend", "dividen_yield.csv"]
    beta_ = ["beta", "beta.csv"]
    gross_m_ = ["gross_m", "gross_margin.csv"]
    oper_m_ = ["oper_m", "operating_margin.csv"]
    ernqtrgrth_ = ["ernqtrgrth", "earn_quarterly_growth.csv"]
    davgchg200_ = ["davgchg200", "200D_avg_change.csv"]
    wkhchg52_ = ["wkhchg52", "52W_high_change.csv"]
    wklchg52_ = ["wklchg52", "52W_low_change.csv"]
    recomm = ["recomm", "analyst_recommendations.csv"]

    country_ = f'{country_[0]}={add_value(country_[1], items.country)}'
    industry_ = f'&{industry_[0]}={add_value(industry_[1], items.industry)}'
    sector = f'&{sector[0]}={add_value(sector[1], items.sector)}'
    market_cap_ = f'&{market_cap_[0]}={add_value(market_cap_[1], items.marketCap)}'
    percent_change_ = f'&{percent_change_[0]}={add_value(percent_change_[1], items.changePercent)}'
    price_ = f'&{price_[0]}={add_value(price_[1], items.price)}'
    volume_ = f'&{volume_[0]}={add_value(volume_[1], items.volume)}'
    pe_ = f'&{pe_[0]}={add_value(pe_[1], items.pe)}'
    fwd_pe_ = f'&{fwd_pe_[0]}={add_value(fwd_pe_[1], items.forwardPE)}'
    pb_ = f'&{pb_[0]}={add_value(pb_[1], items.priceBook)}'
    peg_ = f'&{peg_[0]}={add_value(peg_[1], items.peg)}'
    earnings_ = f'&{earnings_[0]}={add_value(earnings_[1], items.earnings)}'
    profit_m_ = f'&{profit_m_[0]}={add_value(profit_m_[1], items.profitMargin)}'
    davgchg50_ = f'&{davgchg50_[0]}={add_value(davgchg50_[1], items.avgChg50D)}'
    roa_ = f'&{roa_[0]}={add_value(roa_[1], items.returnOnAssets)}'
    epsy_ = f'&{epsy_[0]}={add_value(epsy_[1], items.epsThisYear)}'
    flt_ = f'&{flt_[0]}={add_value(flt_[1], items.float)}'
    roe_ = f'&{roe_[0]}={add_value(roe_[1], items.returnOnEquity)}'
    epsny_ = f'&{epsny_[0]}={add_value(epsny_[1], items.epsNextYear)}'
    fltsht_ = f'&{fltsht_[0]}={add_value(fltsht_[1], items.floatShort)}'
    curr_r_ = f'&{curr_r_[0]}={add_value(curr_r_[1], items.currentRatio)}'
    epsp5y_ = f'&{epsp5y_[0]}={add_value(epsp5y_[1], items.epsPast5Year)}'
    outstd_ = f'&{outstd_[0]}={add_value(outstd_[1], items.sharesOutstanding)}'
    debteq_ = f'&{debteq_[0]}={add_value(debteq_[1], items.debtEquity)}'
    epsn5y_ = f'&{epsn5y_[0]}={add_value(epsn5y_[1], items.epsNext5Year)}'
    insido_ = f'&{insido_[0]}={add_value(insido_[1], items.insiderOwn)}'
    dividend_ = f'&{dividend_[0]}={add_value(dividend_[1], items.dividendYield)}'
    beta_ = f'&{beta_[0]}={add_value(beta_[1], items.beta)}'
    gross_m_ = f'&{gross_m_[0]}={add_value(gross_m_[1], items.grossMargin)}'
    oper_m_ = f'&{oper_m_[0]}={add_value(oper_m_[1], items.operatingMargin)}'
    ernqtrgrth_ = f'&{ernqtrgrth_[0]}={add_value(ernqtrgrth_[1], items.earnQuarterlyGrowth)}'
    davgchg200_ = f'&{davgchg200_[0]}={add_value(davgchg200_[1], items.avgChg200D)}'
    wkhchg52_ = f'&{wkhchg52_[0]}={add_value(wkhchg52_[1], items.highChg52W)}'
    wklchg52_ = f'&{wklchg52_[0]}={add_value(wklchg52_[1], items.lowChg52W)}'
    recomm = f'&{recomm[0]}={add_value(recomm[1], items.analystRecom)}'

    result = country_ + industry_ + sector + market_cap_ + percent_change_ + price_ + volume_ + pe_ + fwd_pe_ + pb_ + peg_ + earnings_ + profit_m_ + davgchg50_ + roa_ + epsy_ + flt_ + roe_ + epsny_ + fltsht_ + curr_r_ + epsp5y_ + outstd_ + debteq_ + epsn5y_ + insido_ + dividend_ + beta_ + gross_m_ + oper_m_ + ernqtrgrth_ + davgchg200_ + wkhchg52_ + wklchg52_ + recomm + default
    return result
