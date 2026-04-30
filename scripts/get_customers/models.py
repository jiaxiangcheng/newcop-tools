from pydantic import BaseModel
from typing import List


META_CSV_HEADERS = [
    "email", "email", "email",
    "phone", "phone", "phone",
    "madid",
    "fn", "ln",
    "zip", "ct", "st", "country",
    "dob", "doby", "gen", "age",
    "uid",
]


class MetaAudienceRow(BaseModel):
    email: str = ""
    phone: str = ""
    fn: str = ""
    ln: str = ""
    zip: str = ""
    ct: str = ""
    st: str = ""
    country: str = ""
    uid: str = ""

    def to_csv_row(self) -> list:
        return [
            self.email, "", "",
            self.phone, "", "",
            "",
            self.fn, self.ln,
            self.zip, self.ct, self.st, self.country,
            "", "", "", "",
            self.uid,
        ]


class CustomerExportResult(BaseModel):
    visitors: List[MetaAudienceRow] = []
    customers: List[MetaAudienceRow] = []
    total_fetched: int = 0
    execution_time_seconds: float = 0.0
