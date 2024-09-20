import dataclasses
from requests import Response


@dataclasses.dataclass
class BaseRequest:
    pass


@dataclasses.dataclass
class BaseResponse:
    content_type: str = dataclasses.field()
    status_code: int = dataclasses.field()
    raw_response: Response = dataclasses.field()
