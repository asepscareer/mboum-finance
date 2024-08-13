from pydantic import BaseModel


class Item(BaseModel):
    symbol: str

    class Config:
        schema_extra = {
            "example": {
                "symbol": "AAPL",
            }
        }


class Items(BaseModel):
    symbol: str
    key: str


class PriceCustom(BaseModel):
    symbol: str
    start: str
    end: str

