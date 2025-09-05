from enum import Enum


class CaseInsensitiveStrEnum(str, Enum):
    @classmethod
    def _missing_(cls, value):
        for member in cls:
            if member.value.lower() == value.lower():
                return member
        raise ValueError(f"Invalid value: {value}")

    def __str__(self):
        return str(self.value)
