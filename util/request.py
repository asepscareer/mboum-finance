from typing import Optional

from pydantic import BaseModel


class MultipleScreenerItem(BaseModel):
    country: str
    sector: str
    volume: str
    changePercent: str
    marketCap: str
    price: str
    industry: Optional[str] = None
    pe: Optional[str] = None
    forwardPE: Optional[str] = None
    priceBook: Optional[str] = None
    peg: Optional[str] = None
    earnings: Optional[str] = None
    profitMargin: Optional[str] = None
    returnOnAssets: Optional[str] = None
    returnOnEquity: Optional[str] = None
    currentRatio: Optional[str] = None
    debtEquity: Optional[str] = None
    dividendYield: Optional[str] = None
    grossMargin: Optional[str] = None
    epsThisYear: Optional[str] = None
    epsNextYear: Optional[str] = None
    epsPast5Year: Optional[str] = None
    epsNext5Year: Optional[str] = None
    beta: Optional[str] = None
    operatingMargin: Optional[str] = None
    float: Optional[str] = None
    floatShort: Optional[str] = None
    sharesOutstanding: Optional[str] = None
    insiderOwn: Optional[str] = None
    institutionalOwn: Optional[str] = None
    earnQuarterlyGrowth: Optional[str] = None
    avgChg50D: Optional[str] = None
    avgChg200D: Optional[str] = None
    highChg52W: Optional[str] = None
    lowChg52W: Optional[str] = None
    analystRecom: Optional[str] = None
