from pydantic import BaseModel


class Secret(BaseModel):
    """
    Secret response object

    Attributes:
        key (str): Secret key
    """

    key: str


class ListSecretsResponse(BaseModel):
    """
    Response from the list secrets endpoint.

    Attributes:
        total_amount (int): Total amount of the secrets.
        secrets (list[Secret]): List of secrets.
    """

    total_amount: int
    secrets: list[Secret]
