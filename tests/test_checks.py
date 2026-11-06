import pytest

from quality.checks import rule_fails, rule_name


def test_rule_name_with_several_columns():
    rule = {"rule": "not_null", "columns": ["log_id", "user_id"]}
    assert rule_name(rule) == "not_null:log_id,user_id"


def test_rule_name_with_one_column():
    rule = {"rule": "regex", "column": "user_email", "pattern": ".*"}
    assert rule_name(rule) == "regex:user_email"


def test_unknown_rule_type_is_rejected():
    rule = {"rule": "not_nul", "columns": ["log_id"]}
    with pytest.raises(ValueError):
        rule_fails(rule, cfg={})