from lib.column import Column
from lib.schema import Schema
from lib.table import Table
from lib.expected_result import ExpectedResult
from lib.stats import Stats
from lib import assets
from types import SimpleNamespace
from helpers.config import config
from copy import deepcopy


class SchemaDefinition:
    def __init__(self, schema_name):
        self._schema_handle = Schema(schema_name)
        self._table_handle = None
        self._column_handle = None
        self.environments = config()["environments"]
        self.schemas_per_environment = []

    def table(self, table_name):
        """Adds table to schema with given name"""
        self._save_column()
        self._save_table()
        assert not [
            table for table in self._schema_handle.tables if table.name == table_name
        ], "You tried to add a table which already exists"
        self._table_handle = Table(table_name)
        return self

    def _save_table(self):
        if self._table_handle:
            self._schema_handle.tables.append(self._table_handle)
        self._table_handle = None

    def column(self, column_name, column_type):
        """Adds column to current table with given name and type"""
        self._save_column()
        assert self._table_handle, "Add table first, before adding column"
        assert not [
            column
            for column in self._table_handle.columns
            if column.name == column_name
        ], "You tried to add a column which already exists"
        self._column_handle = Column(column_name, column_type)
        return self

    def _save_column(self):
        if self._column_handle:
            self._table_handle.columns.append(self._column_handle)
        self._column_handle = None

    def _prevent_adding_properties_to_non_existent_table(self):
        assert self._table_handle, "Tried to add property to not initialized table"

    def _prevent_adding_properties_to_non_existent_column(self):
        assert self._column_handle, "Tried to add property to not initialized column"

    def _prevent_adding_properties_to_non_existent_entity(self):
        self._prevent_adding_properties_to_non_existent_table()
        self._prevent_adding_properties_to_non_existent_column()

    def allowed_values(self, allowed_values):
        """Adds list of allowed values to column"""
        self._prevent_adding_properties_to_non_existent_entity()

        assert type(allowed_values) is list, "Allowed values must be a list"
        assert allowed_values, "Allowed values cannot be empty"
        assert (
            not self._column_handle.allowed_values
        ), "You already defined allowed values for this column"

        self._column_handle.allowed_values = allowed_values
        return self

    def can_be_null(self):
        """Defines column can contain null values"""
        self._prevent_adding_properties_to_non_existent_entity()

        assert not self._column_handle.null, "You already defined column can be null"

        self._column_handle.null = True
        return self

    def can_be_empty(self):
        """Defines column can contain empty string values"""
        self._prevent_adding_properties_to_non_existent_entity()

        assert not self._column_handle.empty, "You already defined column can be empty"

        self._column_handle.empty = True
        return self

    def can_be_empty_null(self):
        """Shortcut method to define both EMPTY and NULL values for column"""
        return self.can_be_empty().can_be_null()

    def unique(self):
        """Expect values in column to be unique"""
        self._prevent_adding_properties_to_non_existent_entity()

        assert (
            not self._column_handle.unique
        ), "Unique property is already defined for this column"

        self._column_handle.unique = True
        return self

    def skip(self, skip_reason):
        """Skip testing of column. Provide a reason."""
        self._prevent_adding_properties_to_non_existent_entity()

        assert skip_reason, "Skip reason cannot be empty"

        assert (
            not self._column_handle.skip
        ), "Skip property is already defined for this column"

        self._column_handle.skip = skip_reason
        return self

    def min_value(self, value):
        """Define minimal value for column"""
        self._prevent_adding_properties_to_non_existent_entity()

        assert (
            not self._column_handle.min_value
        ), "Min value property is already defined for this column"

        self._column_handle.min_value = value
        return self

    def max_value(self, value):
        """Define maximal value for column"""
        self._prevent_adding_properties_to_non_existent_entity()

        assert (
            not self._column_handle.max_value
        ), "Max value property is already defined for this column"

        self._column_handle.max_value = value
        return self

    def expected_format(self, expected_format):
        """Some string/text columns may contain data in one of expected formats."""
        self._prevent_adding_properties_to_non_existent_entity()

        assert expected_format in assets.EXPECTED_FORMATS
        assert (
            not self._column_handle.expected_format
        ), "Expected formats property is already defined for this column"

        self._column_handle.expected_format = expected_format
        return self

    def collect_stat(self, stat):
        """Write statistical data about column"""
        self._prevent_adding_properties_to_non_existent_entity()

        assert stat in Stats, "Passed stat is not valid stats object"
        assert (
            stat not in self._column_handle.collect_stats
        ), "You already added this stat to column definition"

        self._column_handle.collect_stats.append(stat)
        return self

    def stat_always_grow(self):
        """Expect value of stat to always increase"""
        self._prevent_adding_properties_to_non_existent_entity()

        self._column_handle.stat_always_grow = True
        return self

    def expected_result(self, stat, **expected_result_per_environment):
        """For tests, which are run after processing your workflow on fixed set of data, always expect those numbers"""
        self._prevent_adding_properties_to_non_existent_entity()

        assert stat in Stats, "Given stat does not exists in Stats"

        assert all(
            environment in expected_result_per_environment.keys()
            for environment in self.environments
        ), "Define expected result for every environment from config file"

        for expected_stat_value in expected_result_per_environment.values():
            assert isinstance(expected_stat_value, int), "Passed stat is not an integer"

        for expected_stat in self._column_handle.expected_result:
            assert expected_stat.stat != stat, "You already added this stat to column"

        for environment, expected_result in expected_result_per_environment.items():
            self._column_handle.expected_result.append(
                ExpectedResult(stat, environment, expected_result)
            )

        return self

    def unique_columns_group(self, unique_columns_group):
        """Expect unique combinations of values in two or more columns"""
        self._prevent_adding_properties_to_non_existent_table()
        assert not self._column_handle, "You cannot add unique columns group to a column. Only to a table."

        self._table_handle.unique_columns_group = unique_columns_group
        return self

    def close(self):
        """Run after defining all tables and columns in schema"""
        assert (
            self._column_handle
        ), "You tried to close schema, without defining column for last table"
        self._save_column()
        self._save_table()
        for environment in self.environments:
            self.schemas_per_environment.append(
                SimpleNamespace(environment=environment, schema=deepcopy(self._schema_handle))
            )

        self._schema_handle = None

    def _schema_for_environment_pointer(self, environment):
        return next(
            index
            for index, schema in enumerate(self.schemas_per_environment)
            if schema.environment == environment
        )

    def schema_for_environment(self, environment):
        return self.schemas_per_environment[self._schema_for_environment_pointer(environment)].schema

    def environment_difference(self, environment, table, columns, difference):
        """Add difference between original schema definition

        Args:
            environment - str; for which env you add it
            table - str
            columns - str or list; provide list of columns to add same difference across many
            difference - list of tuples; ex. [("unique": False), ("skip": "Bad data in production")]
            See column.py for possible attributes of column
        """
        assert not self._schema_handle, "Close schema before adding difference"
        assert environment in self.environments, "Given environment does not exist"
        assert difference and isinstance(
            difference, list
        ), "Provide list of tuples as a difference"

        columns = columns if isinstance(columns, list) else [columns]

        schema_pointer = self._schema_for_environment_pointer(environment)
        table_pointer = self.schemas_per_environment[schema_pointer].schema.table_pointer(
            table
        )

        for column in columns:
            column_pointer = (
                self.schemas_per_environment[schema_pointer]
                .schema
                .tables[table_pointer]
                .column_pointer(column)
            )
            for attribute, value in difference:
                assert hasattr(
                    self.schemas_per_environment[schema_pointer]
                    .schema
                    .tables[table_pointer]
                    .columns[column_pointer],
                    attribute,
                ), "You tried to setup column attribute which does not exists"
                assert attribute != "name", "Changing column name is forbidden"

                setattr(
                    self.schemas_per_environment[schema_pointer]
                    .schema
                    .tables[table_pointer]
                    .columns[column_pointer],
                    attribute,
                    value,
                )
