
from __future__ import annotations
import dataclasses
from dataclasses_json import Undefined, dataclass_json
from glassflow import utils


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclasses.dataclass
class Pipeline:
    name: str = dataclasses.field(metadata={'dataclasses_json': { 'letter_case': utils.get_field_name('name') }})
    id: str = dataclasses.field(metadata={'dataclasses_json': { 'letter_case': utils.get_field_name('id') }})
