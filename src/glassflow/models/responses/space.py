from datetime import datetime

from pydantic import BaseModel


class Space(BaseModel):
    """
    Space response object.

    Attributes:
        name (str): Space name.
        id (int): Space id.
        created_at (datetime): Space creation date.
        permission (str): Space permission.
    """

    name: str
    id: str
    created_at: datetime
    permission: str


class ListSpacesResponse(BaseModel):
    """
    Response from list spaces endpoint.

    Attributes:
        total_amount (int): Total amount of spaces.
        spaces (list[Space]): List of spaces.
    """

    total_amount: int
    spaces: list[Space]
