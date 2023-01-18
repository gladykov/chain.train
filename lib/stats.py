from strenum import StrEnum


class Stats(StrEnum):
    TOTAL = "total"  # Counts all
    DISTINCT = "distinct"  # Counts distinct
    TOTAL_IN_RANGE = "total_in_range"  # Counts all with extra WHERE condition set by row_limiter()
    DISTINCT_IN_RANGE = "distinct_in_range" # Counts distinct with extra WHERE condition set by row_limiter()

    @classmethod
    def __iter__(cls):
        for value in cls._member_map_.items():
            yield value.value
