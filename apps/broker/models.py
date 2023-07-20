from pydantic import BaseModel


class Record(BaseModel):
    id: str
    data: str
