from __future__ import annotations
import dataclasses
from dataclasses_json import Undefined, dataclass_json
from glassflow import utils


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclasses.dataclass
class Error(Exception):
    """Bad request error response

    Attributes:
        message: A message describing the error

    """
    message: str = dataclasses.field(metadata={
        'dataclasses_json': {
            'letter_case': utils.get_field_name('message')
        }
    })

    def __str__(self) -> str:
        return utils.marshal_json(self, type(self))
