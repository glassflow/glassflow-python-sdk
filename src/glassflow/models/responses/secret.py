from pydantic import BaseModel


class Secret(BaseModel):
    key: str


class ListSecretsResponse(BaseModel):
    total_amount: int
    secrets: list[Secret]
