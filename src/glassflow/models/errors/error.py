from pydantic import BaseModel

class Error(BaseModel):
    """Bad request error response

    Attributes:
        detail: A message describing the error
    """
    detail: str

    def __str__(self) -> str:
        return self.model_dump_json()
