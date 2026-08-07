from datetime import datetime

import pydantic as pyd


class DataPointAggregations(pyd.BaseModel):
    count: int
    min: float | None
    p25: float | None
    median: float | None
    p75: float | None
    max: float | None
    first_year: int | None
    last_year: int | None
    first_datetime: datetime | None
    last_datetime: datetime | None
    categories: list[str]
    types: list[str]

    model_config = pyd.ConfigDict(extra="ignore")
