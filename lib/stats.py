from strenum import StrEnum


class Stats(StrEnum):
    TOTAL = "total"
    DISTINCT = "distinct"
    TOTAL_IN_RANGE = "total_in_range"
    DISTINCT_IN_RANGE = "distinct_in_range"

    @classmethod
    def __iter__(cls):
        for value in cls._member_map_.items():
            yield value.value
