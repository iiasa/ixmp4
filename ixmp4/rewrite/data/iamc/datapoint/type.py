import enum


class Type(str, enum.Enum):
    BASE = "BASE"
    ANNUAL = "ANNUAL"
    CATEGORICAL = "CATEGORICAL"
    DATETIME = "DATETIME"
