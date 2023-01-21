<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [chain.train](#chaintrain)
  - [Quickstart](#quickstart)
  - [Overview](#overview)
  - [Environments](#environments)
  - [Creating schema](#creating-schema)
  - [Running schema tests](#running-schema-tests)
  - [Running statistical tests](#running-statistical-tests)
  - [Running regression tests](#running-regression-tests)
  - [Limiting amount of processed data](#limiting-amount-of-processed-data)
  - [About connectors](#about-connectors)
  - [Bugs and contributions](#bugs-and-contributions)
  - [I have a big project, can you help me setup this stuff?](#i-have-a-big-project-can-you-help-me-setup-this-stuff)
  - [Code](#code)
  - [Troubleshoot](#troubleshoot)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# chain.train

ETL & database testing framework for big data, written with Python, using Pytest. With connectors for Spark, Snowflake, MySQL/MariaDB.

```
from lib.schema_definition import SchemaDefinition
from lib.stats import Stats

schema_definition = SchemaDefinition("TEST_SCHEMA")

(
    schema_definition.table("TEST_TABLE_1")
    .row_limiter("COLUMN_NAME_2 BETWEEN 2010-05-21 AND 2010-06-21")
    .column("COLUMN_NAME", "text")
    .unique()
    .collect_stat(Stats.TOTAL_IN_RANGE)
    .expected_result(Stats.DISTINCT, qa=3, production=456)
    .expected_format("guid")
    .column("COLUMN_NAME_1", "text")
    .allowed_values(["duck", "dog", "cat"])
    .column("COLUMN_NAME_2", "int")
    .can_be_null()
    .column("COLUMN_NAME_3", "bigint")
    .skip("JIRA-1234 Bad data")
)

(
    schema_definition.table("TEST_TABLE_2")...
)

schema_definition.close()

schema_definition.environment_difference("production", "TEST_TABLE_1", "COLUMN_NAME_3", [("skip", False), ("min_value", 256)])
```

## Quickstart

1. Install requirements.txt
2. Install requirements_{connector} specific for your DB setup
3. Copy `config.toml.template` to `config.toml` and update connection values
4. Create your schema definition
5. Run tests: ```python -m pytest -s --schema_name=TEST_SCHEMA --env=qa tests/test_schema.py```

## Overview

chain.train is a testing framework for big data, to validate schemas, gather statistical data about produced output tables, run statistical tests, and also run tests on fixed set's of data, for regression testing.
Created with reusability in mind and possible improvements in the future. It takes into account existence of different environments like qa, staging, production and differences which may exists between them.

Core of framework is schema definition, which defines all tables, columns, their properties, types of statistical data to be gathered and expected counts for regression tests.

Framework contains row limiters and samplers - when used will limit amount of data fetched, to speed up testing and not cause memory overflows with huge datasets.

## Environments

Are defined in `config.toml` . You can provide arbitrary number and names of the,.

## Creating schema

Create new schema in `schemas` dir. Name of file must be equal to name of schema.

You can define your schema based on any environment. After you will `close()` your schema definition, you can add environment specific differences.
Note: Snowflake loves to return column names and types in UPPERCASE, even if you define them in lowercase. 

### table

```python
def table(table_name)
```

Adds table to schema with given name

### column

```python
def column(column_name, column_type)
```

Adds column to current table with given name and type


### allowed\_values

```python
def allowed_values(allowed_values)
```

Adds list of allowed values to column

### can\_be\_null

```python
def can_be_null()
```

Defines column can contain null values

### can\_be\_empty

```python
def can_be_empty()
```

Defines column can contain empty string values

### can\_be\_empty\_null

```python
def can_be_empty_null()
```

Shortcut method; column may contain both EMPTY and NULL

### unique

```python
def unique()
```

Expect values in column to be unique

### skip

```python
def skip(skip_reason)
```

Skip testing of column. Provide a reason. Reason will be printed when test runs

### min\_value

```python
def min_value(value)
```

Define minimal value for column

### max\_value

```python
def max_value(value)
```

Define maximal value for column

### expected\_format

```python
def expected_format(expected_format)
```

Some string/text columns may contain string data, which may be parsed to one of expected formats.

### collect\_stat

```python
def collect_stat(stat)
```

Gather statistical data about column. Pass Stats object from stats.py ex .collect_stat(Stat.DISTINCT). Can be called more than once for single column, to add more than one stat.

### stat\_always\_grow

```python
def stat_always_grow()
```

Expect value of stat to always increase

### expected\_result

```python
def expected_result(stat, **expected_count_per_environment)
```

For tests, which are run after processing your workflow on fixed set of data, always expect those numbers

### unique\_columns\_group

```python
def unique_columns_group(unique_columns_group)
```

Expect unique combinations of values in two or more columns

### row\_limiter

```python
def row_limiter(row_limiter)
```

For big tables add extra WHERE condition to limit amount of data pulled into test. Useful also when you run ETL periodically and want to test rows produced during latest run.


### close

```python
def close()
```

Run after defining all tables and columns in schema.

### environment\_difference

```python
def environment_difference(environment, table, columns, difference)
```

Add difference between original schema definition

**Arguments**:

  environment - str; for which env you add it
  table - str
  columns - str or list; provide list of columns to add same difference across many
  difference - list of tuples; ex. [("unique": False), ("skip": "Bad data in production")]
  See column.py for possible attributes of column
  
## Running schema tests

This will validate table names, column names and types, null and empty values, allowed values, expected formats, etc.. For full list of tests see `tests/test_schema.py`

```python -m pytest -s --schema_name=TEST_SCHEMA --env=qa tests/test_schema.py```

## Running statistical tests

### Gather stats

First you need to gather some statistical data about your tables, but running few times (define how much in `test_statistics.py`):

`python -m tests.gather_statistics --schema_name=TEST_SCHEMA --env=qa`

This will execute all counts defined for column, and write results to {SCHEMA_NAME}_statistics.statistics table

Q: Why you will not do count only once for whole table? 
A: Because NULLs are not counted when counting single column.  

Q: Why stats are in separate schema with single table?
A: To keep it as separate as possible, not to pollute production schemas with test data, and not to introduce differences, depending where you will run it.

Q: Why not single schema for all stats?
A: With many schemas in play, it will be easier to migrate some of them. Also - performance. We always query for whole statistics table. It is very small, so it is OK.

### Test stats

`python -m pytest -s --schema_name=TEST_SCHEMA --env=qa tests/test_statistics.py`

This will execute set of tests comparing counts. Implemented tests should be treated more like an example:

* test_standard_deviation
* test_stat_always_grow
* test_latest_run_is_not_zero

If there are not enough defined data points for comparison, it will also pass.

## Running regression tests

If you have ability to run your ETL pipeline on fixed set of data, you should always expect same counts in output data.
This can be achieved by creating snapshot of existing data or limiting input data based on some condition, ex. date.

Define expected counts for column:
```
   .column("COLUMN_NAME", "text")
   .expected_result(Stats.DISTINCT, qa=3, production=456)
```

and then run:

`python -m pytest -s --schema_name=TEST_SCHEMA --env=qa tests/test_regression.py`

Tests expect output tables to live in `{SCHEMA_NAME}_regression`

## Limiting amount of processed data

For big big data, tests may either take long time or cause memory errors. Right now percentage of data processed may be limited in two ways:

For `expected_format` test by fetching only x percentage of table, before ordering by rand(). `sample_subset_percentage` is defined in `config.toml`.
Because of implementation details, `sample()` methods are different for different connectors.

For all other tests: by passing `row_limiter` to table. This is just another `WHERE` clause which is appended to queries.
This way you can select based on periods, partitions, etc. Or you attach another percentage limiter, by passing `rand() <= {subset_percentage}`

## About connectors

Connectors have all read and write operations implemented, but you should not be tempted to reuse write functionality to drive your ETLs - they are nowhere efficient for big data operations. Just good enough to write small number of rows. 
Where tested with MySQL, MariaDB, Spark 3.x and Snowflake with Python 3.9, and Spark 2.x and Python 3.6

Q: Why you return custom objects instead of Pandas?
A: Good point. I thought about it too late. Something to improve in the future.

Q: I want to store secrets as env variables / fetch from secret manager
A: Please generate `config.toml` on the fly and remove it after. If you know some nice abstract method to manage them, please let me know.

## Bugs and contributions

Please tell me about bugs, ideas and please make contributions. There are many use cases which I didn't thought of or didn't implement yet. Goal here is to be universal out-of-the box experience.
Main assumption to follow:
- do as many checks as possible when defining schema - running tests takes time, so we want to catch anything before executing actual queries

## I have a big project, can you help me setup this stuff?

Drop a message to my parent company Modus Create

## Code

Black, Isort, Bugbear - all will kindly yell at you from Github Actions

## Troubleshoot

Long chain trains may cause errors: https://github.com/PyCQA/flake8-bugbear/issues/295

