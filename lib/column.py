class Column:
    def __init__(self, name, column_type):
        self.name = name
        self.type = column_type
        self.unique = False
        self.null = False
        self.empty = False
        self.expected_format = None
        self.allowed_values = None
        self.min_value = None
        self.max_value = None
        self.skip = False
        self.gather_stats = []
        self.stat_always_grow = False
        self.expected_results = []

    def expected_results_for_environment(self, env):
        return [
            expected_result
            for expected_result in self.expected_results
            if expected_result.environment == env
        ]
