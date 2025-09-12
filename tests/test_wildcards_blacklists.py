import pytest

from src.jsonsql import JsonSQL


class TestWildcardSupport:
    """Test wildcard functionality for allowing all entities."""

    def test_wildcard_queries(self):
        """Test wildcard support for queries."""
        jsonsql = JsonSQL(
            allowed_queries=["*"],
            allowed_items=["*"],
            allowed_tables=["*"],
            allowed_connections=["*"],
            allowed_columns={"*": object}
        )

        # Should allow any query type
        input_data = {
            "query": "SELECT",
            "items": ["name"],
            "table": "users"
        }
        result, sql, params = jsonsql.sql_parse(input_data)
        assert result is True
        assert sql == "SELECT name FROM users"

        # Should allow INSERT as well
        input_data["query"] = "INSERT"
        result, sql, params = jsonsql.sql_parse(input_data)
        assert result is True

    def test_wildcard_items(self):
        """Test wildcard support for items."""
        jsonsql = JsonSQL(
            allowed_queries=["SELECT"],
            allowed_items=["*"],
            allowed_tables=["users"],
            allowed_columns={"*": object}
        )

        input_data = {
            "query": "SELECT",
            "items": ["name", "email", "any_column"],
            "table": "users"
        }
        result, sql, params = jsonsql.sql_parse(input_data)
        assert result is True
        assert sql == "SELECT name,email,any_column FROM users"

    def test_wildcard_tables(self):
        """Test wildcard support for tables."""
        jsonsql = JsonSQL(
            allowed_queries=["SELECT"],
            allowed_items=["*"],
            allowed_tables=["*"],
            allowed_columns={"*": object}
        )

        input_data = {
            "query": "SELECT",
            "items": ["*"],
            "table": "any_table"
        }
        result, sql, params = jsonsql.sql_parse(input_data)
        assert result is True
        assert sql == "SELECT * FROM any_table"

    def test_wildcard_columns(self):
        """Test wildcard support for columns."""
        jsonsql = JsonSQL(
            allowed_queries=["SELECT"],
            allowed_items=["*"],
            allowed_tables=["users"],
            allowed_connections=["WHERE"],
            allowed_columns={"*": object}
        )

        input_data = {
            "query": "SELECT",
            "items": ["*"],
            "table": "users",
            "connection": "WHERE",
            "logic": {"any_column": {"=": "test value"}}
        }
        result, sql, params = jsonsql.sql_parse(input_data)
        assert result is True
        assert sql == "SELECT * FROM users WHERE any_column = ?"
        assert params == ("test value",)


class TestBlacklistSupport:
    """Test blacklist functionality for explicitly forbidden entities."""

    def test_blacklist_queries(self):
        """Test blacklist support for queries."""
        jsonsql = JsonSQL(
            allowed_queries=["*"],
            allowed_items=["*"],
            allowed_tables=["*"],
            not_allowed_queries=["DROP", "DELETE"]
        )

        # Should allow SELECT
        input_data = {
            "query": "SELECT",
            "items": ["*"],
            "table": "users"
        }
        result, sql, params = jsonsql.sql_parse(input_data)
        assert result is True

        # Should deny DROP
        input_data["query"] = "DROP"
        result, msg = jsonsql.sql_parse(input_data)
        assert result is False
        assert "Query not allowed - DROP" in msg

        # Should deny DELETE
        input_data["query"] = "DELETE"
        result, msg = jsonsql.sql_parse(input_data)
        assert result is False
        assert "Query not allowed - DELETE" in msg

    def test_blacklist_items(self):
        """Test blacklist support for items."""
        jsonsql = JsonSQL(
            allowed_queries=["SELECT"],
            allowed_items=["*"],
            allowed_tables=["users"],
            not_allowed_items=["password", "secret_key"]
        )

        # Should allow normal columns
        input_data = {
            "query": "SELECT",
            "items": ["name", "email"],
            "table": "users"
        }
        result, sql, params = jsonsql.sql_parse(input_data)
        assert result is True

        # Should deny password
        input_data["items"] = ["name", "password"]
        result, msg = jsonsql.sql_parse(input_data)
        assert result is False
        assert "Item not allowed - password" in msg

    def test_blacklist_tables(self):
        """Test blacklist support for tables."""
        jsonsql = JsonSQL(
            allowed_queries=["SELECT"],
            allowed_items=["*"],
            allowed_tables=["*"],
            not_allowed_tables=["admin_secrets", "system_config"]
        )

        # Should allow normal tables
        input_data = {
            "query": "SELECT",
            "items": ["*"],
            "table": "users"
        }
        result, sql, params = jsonsql.sql_parse(input_data)
        assert result is True

        # Should deny blacklisted table
        input_data["table"] = "admin_secrets"
        result, msg = jsonsql.sql_parse(input_data)
        assert result is False
        assert "Table not allowed - admin_secrets" in msg

    def test_blacklist_columns(self):
        """Test blacklist support for columns."""
        jsonsql = JsonSQL(
            allowed_queries=["SELECT"],
            allowed_items=["*"],
            allowed_tables=["users"],
            allowed_connections=["WHERE"],
            allowed_columns={"*": object},
            not_allowed_columns=["password", "secret_token"]
        )

        # Should allow normal columns
        input_data = {
            "query": "SELECT",
            "items": ["*"],
            "table": "users",
            "connection": "WHERE",
            "logic": {"name": {"=": "John"}}
        }
        result, sql, params = jsonsql.sql_parse(input_data)
        assert result is True

        # Should deny blacklisted column
        input_data["logic"] = {"password": {"=": "secret"}}
        result, msg = jsonsql.sql_parse(input_data)
        assert result is False
        assert "Invalid Input - password" in msg


class TestStrictModeDefault:
    """Test that strict mode is the default behavior."""

    def test_empty_allowed_lists_deny_all(self):
        """Test that empty allowed lists deny everything (strict mode)."""
        jsonsql = JsonSQL()  # All lists empty by default

        input_data = {
            "query": "SELECT",
            "items": ["*"],
            "table": "users"
        }
        result, msg = jsonsql.sql_parse(input_data)
        assert result is False
        assert "Query not allowed - SELECT" in msg

    def test_explicit_empty_lists(self):
        """Test explicit empty lists maintain strict behavior."""
        jsonsql = JsonSQL(
            allowed_queries=[],
            allowed_items=[],
            allowed_tables=[],
            allowed_connections=[],
            allowed_columns={}
        )

        input_data = {
            "query": "SELECT",
            "items": ["name"],
            "table": "users"
        }
        result, msg = jsonsql.sql_parse(input_data)
        assert result is False
        assert "Query not allowed - SELECT" in msg


class TestMixedPermissions:
    """Test combinations of wildcards, blacklists, and explicit permissions."""

    def test_wildcard_with_blacklist(self):
        """Test wildcard permissions with blacklist restrictions."""
        jsonsql = JsonSQL(
            allowed_queries=["*"],
            allowed_items=["*"],
            allowed_tables=["*"],
            allowed_connections=["*"],
            allowed_columns={"*": object},
            not_allowed_queries=["DROP"],
            not_allowed_items=["password"],
            not_allowed_tables=["secrets"],
            not_allowed_columns=["secret_key"]
        )

        # Should allow normal operations
        input_data = {
            "query": "SELECT",
            "items": ["name", "email"],
            "table": "users",
            "connection": "WHERE",
            "logic": {"id": {"=": 123}}
        }
        result, sql, params = jsonsql.sql_parse(input_data)
        assert result is True

        # Should deny blacklisted query
        input_data["query"] = "DROP"
        result, msg = jsonsql.sql_parse(input_data)
        assert result is False
        assert "Query not allowed - DROP" in msg

    def test_explicit_with_blacklist(self):
        """Test explicit permissions with blacklist restrictions."""
        jsonsql = JsonSQL(
            allowed_queries=["SELECT", "INSERT", "UPDATE"],
            allowed_items=["name", "email", "age"],
            allowed_tables=["users", "profiles"],
            allowed_connections=["WHERE"],
            allowed_columns={"name": str, "email": str, "age": int},
            not_allowed_queries=["UPDATE"],  # Blacklist overrides whitelist
            not_allowed_items=["age"]
        )

        # Should allow SELECT with allowed items
        input_data = {
            "query": "SELECT",
            "items": ["name", "email"],
            "table": "users"
        }
        result, sql, params = jsonsql.sql_parse(input_data)
        assert result is True

        # Should deny UPDATE even though it's in allowed_queries
        input_data["query"] = "UPDATE"
        result, msg = jsonsql.sql_parse(input_data)
        assert result is False
        assert "Query not allowed - UPDATE" in msg

        # Should deny age even though it's in allowed_items
        input_data["query"] = "SELECT"
        input_data["items"] = ["name", "age"]
        result, msg = jsonsql.sql_parse(input_data)
        assert result is False
        assert "Item not allowed - age" in msg


class TestAggregateValidation:
    """Test aggregate function validation with new system."""

    def test_aggregate_with_wildcard(self):
        """Test aggregate functions with wildcard permissions."""
        jsonsql = JsonSQL(
            allowed_queries=["SELECT"],
            allowed_items=["*"],
            allowed_tables=["users"],
            allowed_columns={"*": object}
        )

        input_data = {
            "query": "SELECT",
            "items": [{"COUNT": "id"}, {"MAX": "age"}],
            "table": "users"
        }
        result, sql, params = jsonsql.sql_parse(input_data)
        assert result is True
        assert sql == "SELECT COUNT(id),MAX(age) FROM users"

    def test_aggregate_with_blacklist(self):
        """Test aggregate functions with blacklisted columns."""
        jsonsql = JsonSQL(
            allowed_queries=["SELECT"],
            allowed_items=["*"],
            allowed_tables=["users"],
            allowed_columns={"*": object},
            not_allowed_items=["password"]
        )

        # Should allow normal aggregate
        input_data = {
            "query": "SELECT",
            "items": [{"COUNT": "id"}],
            "table": "users"
        }
        result, sql, params = jsonsql.sql_parse(input_data)
        assert result is True

        # Should deny aggregate on blacklisted column
        input_data["items"] = [{"COUNT": "password"}]
        result, msg = jsonsql.sql_parse(input_data)
        assert result is False
        assert "Item not allowed - password" in msg


class TestBackwardsCompatibility:
    """Test that existing code continues to work."""

    def test_old_initialization_style(self):
        """Test that old initialization parameters still work."""
        jsonsql = JsonSQL(
            allowed_queries=["SELECT"],
            allowed_items=["name", "email"],
            allowed_tables=["users"],
            allowed_connections=["WHERE"],
            allowed_columns={"name": str, "email": str}
        )

        input_data = {
            "query": "SELECT",
            "items": ["name", "email"],
            "table": "users",
            "connection": "WHERE",
            "logic": {"name": {"=": "John"}}
        }
        result, sql, params = jsonsql.sql_parse(input_data)
        assert result is True
        assert sql == "SELECT name,email FROM users WHERE name = ?"
        assert params == ("John",)

        # Should still deny non-allowed items
        input_data["items"] = ["name", "password"]
        result, msg = jsonsql.sql_parse(input_data)
        assert result is False
        assert "Item not allowed - password" in msg
