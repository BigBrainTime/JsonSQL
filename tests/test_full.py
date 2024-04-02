import pytest
from jsonsql import JsonSQL


@pytest.fixture
def jsonsql() -> JsonSQL:
    return JsonSQL([], [], [], [], {})

# Test SQL Parse


def test_missing_argument(jsonsql: JsonSQL):
    jsonsql.ALLOWED_QUERIES = ["SELECT"]
    jsonsql.ALLOWED_ITEMS = ["*"]
    jsonsql.ALLOWED_TABLES = ["table1"]
    jsonsql.ALLOWED_CONNECTIONS = ["WHERE"]
    input = {"query": "SELECT", "items": ["*"], "connection": "WHERE"}
    result, msg = jsonsql.sql_parse(input)
    assert result is False
    assert msg == "Missing argument table"


def test_bad_query(jsonsql: JsonSQL):
    jsonsql.ALLOWED_QUERIES = ["SELECT"]
    jsonsql.ALLOWED_ITEMS = ["*"]
    jsonsql.ALLOWED_TABLES = ["table1"]
    jsonsql.ALLOWED_CONNECTIONS = ["WHERE"]
    input = {
        "query": "INSERT",
        "items": ["*"],
        "table": "table1",
        "connection": "WHERE",
    }
    result, msg = jsonsql.sql_parse(input)
    assert result is False
    assert msg == "Query not allowed - INSERT"


def test_bad_item(jsonsql: JsonSQL):
    jsonsql.ALLOWED_QUERIES = ["SELECT"]
    jsonsql.ALLOWED_ITEMS = ["good"]
    jsonsql.ALLOWED_TABLES = {"table1":[None]}
    jsonsql.ALLOWED_CONNECTIONS = ["WHERE"]
    input = {
        "query": "SELECT",
        "items": ["bad"],
        "table": "table1",
        "connection": "WHERE",
    }
    result, msg = jsonsql.sql_parse(input)
    assert result is False
    assert msg == "Item not allowed - bad"


def test_bad_table(jsonsql: JsonSQL):
    jsonsql.ALLOWED_QUERIES = ["SELECT"]
    jsonsql.ALLOWED_ITEMS = ["*"]
    jsonsql.ALLOWED_TABLES = ["table1"]
    jsonsql.ALLOWED_CONNECTIONS = ["WHERE"]
    input = {"query": "SELECT", "items": [
        "*"], "table": "bad", "connection": "WHERE"}
    result, msg = jsonsql.sql_parse(input)
    assert result is False
    assert msg == "Table not allowed - bad"


def test_valid_sql_no_logic(jsonsql: JsonSQL):
    jsonsql.ALLOWED_QUERIES = ["SELECT"]
    jsonsql.ALLOWED_ITEMS = ["*"]
    jsonsql.ALLOWED_TABLES = ["table1"]
    input = {"query": "SELECT", "items": ["*"], "table": "table1"}
    result, sql, params = jsonsql.sql_parse(input)
    assert result is True
    assert sql == "SELECT * FROM table1"
    assert params == ()


def test_valid_sql_with_logic(jsonsql: JsonSQL):
    jsonsql.ALLOWED_QUERIES = ["SELECT"]
    jsonsql.ALLOWED_ITEMS = ["*"]
    jsonsql.ALLOWED_TABLES = ["table1"]
    jsonsql.ALLOWED_CONNECTIONS = ["WHERE"]
    jsonsql.ALLOWED_COLUMNS = {"col1": int}
    input = {
        "query": "SELECT",
        "items": ["*"],
        "table": "table1",
        "connection": "WHERE",
        "logic": {"col1": {"<=": 2}},
    }
    result, sql, params = jsonsql.sql_parse(input)
    assert result is True
    assert sql == "SELECT * FROM table1 WHERE col1 <= ?"
    assert params == (2,)


def test_valid_sql_without_logic_with_aggregate_table(jsonsql: JsonSQL):
    jsonsql.ALLOWED_QUERIES = ["SELECT"]
    jsonsql.ALLOWED_ITEMS = ["column"]
    jsonsql.ALLOWED_TABLES = {"table1": [None]}
    input = {"query": "SELECT", "items": [
        {"MIN": "column"}], "table": "table1"}
    result, sql, params = jsonsql.sql_parse(input)
    assert result is True
    assert sql == "SELECT MIN(column) FROM table1"
    assert params == ()


def test_valid_sql_dict_tables(jsonsql: JsonSQL):
    jsonsql.ALLOWED_QUERIES = ["SELECT"]
    jsonsql.ALLOWED_TABLES = {"table1": ["col1"], "table2": ["col2"]}
    input = {"query": "SELECT", "items": ["col1"], "table": "table1"}
    print(jsonsql.sql_parse(input))
    result, sql, params = jsonsql.sql_parse(input)
    assert result is True
    assert sql == "SELECT col1 FROM table1"
    assert params == ()
