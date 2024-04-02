import pytest
from jsonsql import JsonSQL


## Test Logic Parse


@pytest.fixture
def jsonsql() -> JsonSQL:
    return JsonSQL([], [], [], [], {})


def test_invalid_input(jsonsql: JsonSQL):
    input = {"invalid": "value"}
    result, msg = jsonsql.logic_parse(input)
    assert result is False
    assert msg == "Invalid Input - invalid"


def test_bad_and_non_list(jsonsql: JsonSQL):
    input = {"AND": "value"}
    result, msg = jsonsql.logic_parse(input)
    assert result is False
    assert msg == "Bad AND, non list"


def test_bad_column_type(jsonsql: JsonSQL):
    jsonsql.ALLOWED_COLUMNS = {"col1": str}
    input = {"col1": {"=": 123}}
    result, msg = jsonsql.logic_parse(input)
    assert result is False
    assert msg == "Bad col1, non <class 'str'>"


def test_valid_single_column(jsonsql: JsonSQL):
    jsonsql.ALLOWED_COLUMNS = {"col1": str}
    input = {"col1": {"=": "value"}}
    result, sql, params = jsonsql.logic_parse(input)
    assert result is True
    assert sql == "col1 = ?"
    assert params == ("value",)


def test_invalid_boolean_len(jsonsql: JsonSQL):
    input = {"AND": ["cond"]}
    result, msg = jsonsql.logic_parse(input)
    assert result is False
    assert msg == "Invalid boolean length, must be >= 2"


def test_valid_multi_condition(jsonsql: JsonSQL):
    jsonsql.ALLOWED_COLUMNS = {"col1": str, "col2": str}
    input = {"AND": [{"col1": {"=": "value1"}}, {"col2": {"=": "value2"}}]}
    result, sql, params = jsonsql.logic_parse(input)
    assert result is True
    assert sql == "(col1 = ? AND col2 = ?)"
    assert params == ("value1", "value2")


def test_valid_gt_comparison_condition(jsonsql: JsonSQL):
    jsonsql.ALLOWED_COLUMNS = {"col1": int}
    input = {"col1": {">": 10}}
    result, sql, params = jsonsql.logic_parse(input)
    assert result is True
    assert sql == "col1 > ?"
    assert params == (10,)


def test_valid_lt_condition(jsonsql: JsonSQL):
    jsonsql.ALLOWED_COLUMNS = {"col1": int}
    input = {"col1": {"<": 10}}
    result, sql, params = jsonsql.logic_parse(input)
    assert result is True
    assert sql == "col1 < ?"
    assert params == (10,)


def test_valid_gte_condition(jsonsql: JsonSQL):
    jsonsql.ALLOWED_COLUMNS = {"col1": int}
    input = {"col1": {">=": 10}}
    result, sql, params = jsonsql.logic_parse(input)
    assert result is True
    assert sql == "col1 >= ?"
    assert params == (10,)


def test_valid_lte_condition(jsonsql: JsonSQL):
    jsonsql.ALLOWED_COLUMNS = {"col1": int}
    input = {"col1": {"<=": 10}}
    result, sql, params = jsonsql.logic_parse(input)
    assert result is True
    assert sql == "col1 <= ?"
    assert params == (10,)


def test_valid_neq_condition(jsonsql: JsonSQL):
    jsonsql.ALLOWED_COLUMNS = {"col1": int}
    input = {"col1": {"!=": 10}}
    result, sql, params = jsonsql.logic_parse(input)
    assert result is True
    assert sql == "col1 <> ?"
    assert params == (10,)


def test_invalid_operator(jsonsql: JsonSQL):
    jsonsql.ALLOWED_COLUMNS = {"col1": int}
    input = {"col1": {"!": 10}}
    result, msg = jsonsql.logic_parse(input)
    assert result is False
    assert msg == "Non Valid comparitor - !"


def test_valid_between_condition(jsonsql: JsonSQL):
    jsonsql.ALLOWED_COLUMNS = {"col1": int}
    input = {"col1": {"BETWEEN": [5, 10]}}
    result, sql, params = jsonsql.logic_parse(input)
    assert result is True
    assert sql == "col1 BETWEEN ? AND ?"
    assert params == (5, 10)


def test_valid_in_condition(jsonsql: JsonSQL):
    jsonsql.ALLOWED_COLUMNS = {"col1": int}
    input = {"col1": {"IN": [5, 10, 15]}}
    result, sql, params = jsonsql.logic_parse(input)
    assert result is True
    assert sql == "col1 IN (?,?,?)"
    assert params == (5, 10, 15)


def test_valid_eq_condition_column(jsonsql: JsonSQL):
    jsonsql.ALLOWED_COLUMNS = {"col1": int, "col2": int}
    input = {"col1": {"=": "col2"}}
    result, sql, params = jsonsql.logic_parse(input)
    assert result is True
    assert sql == "col1 = col2"
    assert params == ()


def test_valid_gt_condition_column(jsonsql: JsonSQL):
    jsonsql.ALLOWED_COLUMNS = {"col1": int, "col2": int}
    input = {"col1": {">": "col2"}}
    result, sql, params = jsonsql.logic_parse(input)
    assert result is True
    assert sql == "col1 > col2"
    assert params == ()


def test_valid_lt_condition_column(jsonsql: JsonSQL):
    jsonsql.ALLOWED_COLUMNS = {"col1": int, "col2": int}
    input = {"col1": {"<": "col2"}}
    result, sql, params = jsonsql.logic_parse(input)
    assert result is True
    assert sql == "col1 < col2"
    assert params == ()


def test_valid_gte_condition_column(jsonsql: JsonSQL):
    jsonsql.ALLOWED_COLUMNS = {"col1": int, "col2": int}
    input = {"col1": {">=": "col2"}}
    result, sql, params = jsonsql.logic_parse(input)
    assert result is True
    assert sql == "col1 >= col2"
    assert params == ()


def test_valid_lte_condition_column(jsonsql: JsonSQL):
    jsonsql.ALLOWED_COLUMNS = {"col1": int, "col2": int}
    input = {"col1": {"<=": "col2"}}
    result, sql, params = jsonsql.logic_parse(input)
    assert result is True
    assert sql == "col1 <= col2"
    assert params == ()


def test_valid_neq_condition_column(jsonsql: JsonSQL):
    jsonsql.ALLOWED_COLUMNS = {"col1": int, "col2": int}
    input = {"col1": {"!=": "col2"}}
    result, sql, params = jsonsql.logic_parse(input)
    assert result is True
    assert sql == "col1 <> col2"
    assert params == ()


def test_valid_aggregate_condition_column(jsonsql: JsonSQL):
    jsonsql.ALLOWED_COLUMNS = {"col1": int, "col2": int}
    input = {"col1": {"=": {"MIN": "col2"}}}
    result, sql, params = jsonsql.logic_parse(input)
    assert result is True
    assert sql == "col1 = MIN(col2)"
    assert params == ()