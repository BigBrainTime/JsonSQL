from typing import Dict, List, Literal


class JsonSQL:
    def __init__(
        self,
        allowed_queries: List[str] = None,
        allowed_items: List[str] = None,
        allowed_tables: List[str] = None,
        allowed_connections: List[str] = None,
        allowed_columns: Dict[str, type] = None,
        not_allowed_queries: List[str] = None,
        not_allowed_items: List[str] = None,
        not_allowed_tables: List[str] = None,
        not_allowed_connections: List[str] = None,
        not_allowed_columns: List[str] = None
    ):
        """Initializes JsonSQL instance with allowed/not_allowed queries, items,
        tables, connections, and columns.

        Args:
            allowed_queries (List[str], optional): Allowed SQL query strings.
                Use ["*"] to allow all. Defaults to [] (strict mode).
            allowed_items (List[str], optional): Allowed SQL SELECT fields.
                Use ["*"] to allow all. Defaults to [] (strict mode).
            allowed_tables (List[str], optional): Allowed SQL FROM tables.
                Use ["*"] to allow all. Defaults to [] (strict mode).
            allowed_connections (List[str], optional): Allowed SQL JOIN conditions.
                Use ["*"] to allow all. Defaults to [] (strict mode).
            allowed_columns (Dict[str, type], optional): Allowed columns per table.
                Use {"*": object} to allow all. Defaults to {} (strict mode).
            not_allowed_queries (List[str], optional): Explicitly forbidden queries.
                Takes precedence over allowed_queries. Defaults to [].
            not_allowed_items (List[str], optional): Explicitly forbidden items.
                Takes precedence over allowed_items. Defaults to [].
            not_allowed_tables (List[str], optional): Explicitly forbidden tables.
                Takes precedence over allowed_tables. Defaults to [].
            not_allowed_connections (List[str], optional): Explicitly forbidden connections.
                Takes precedence over allowed_connections. Defaults to [].
            not_allowed_columns (List[str], optional): Explicitly forbidden columns.
                Takes precedence over allowed_columns. Defaults to [].
        """
        # Initialize allowed lists with default empty lists for strict mode
        self.ALLOWED_QUERIES = allowed_queries if allowed_queries is not None else []
        self.ALLOWED_ITEMS = allowed_items if allowed_items is not None else []
        self.ALLOWED_CONNECTIONS = allowed_connections if allowed_connections is not None else []
        self.ALLOWED_COLUMNS = allowed_columns if allowed_columns is not None else {}

        # Initialize not_allowed lists
        self.NOT_ALLOWED_QUERIES = not_allowed_queries if not_allowed_queries is not None else []
        self.NOT_ALLOWED_ITEMS = not_allowed_items if not_allowed_items is not None else []
        self.NOT_ALLOWED_TABLES = not_allowed_tables if not_allowed_tables is not None else []
        self.NOT_ALLOWED_CONNECTIONS = not_allowed_connections if not_allowed_connections is not None else []
        self.NOT_ALLOWED_COLUMNS = not_allowed_columns if not_allowed_columns is not None else []

        # Enhanced table handling from dev branch
        allowed_tables = allowed_tables if allowed_tables is not None else []
        table_dict = {}
        for table in allowed_tables:
            if isinstance(table, dict) and isinstance(table[list(table)[0]], list):
                table_dict[list(table)[0]] = table[list(table)[0]]
            elif isinstance(table, dict) and not isinstance(
                table[list(table)[0]], list
            ):
                raise TypeError(f"Table {table} items must be a list")
            elif isinstance(table, str):
                table_dict[table] = [None]
            else:
                raise TypeError(f"{table} not str or dict")

        self.ALLOWED_TABLES = table_dict

        self.LOGICAL = ("AND", "OR")
        self.COMPARISON = ("=", ">", "<", ">=", "<=", "<>", "!=")
        self.SPECIAL_COMPARISON = ("BETWEEN", "IN")
        self.AGGREGATES = ("MIN", "MAX", "SUM", "AVG", "COUNT")

    def _is_entity_allowed(
        self,
        entity: str,
        allowed_list: List[str],
        not_allowed_list: List[str]
    ) -> bool:
        """Check if an entity is allowed based on whitelist and blacklist rules.

        Validation logic:
        1. If entity is in not_allowed_list → DENY (blacklist takes precedence)
        2. If allowed_list is empty → DENY (strict mode)
        3. If allowed_list contains "*" → ALLOW (wildcard mode)
        4. If entity is in allowed_list → ALLOW
        5. Otherwise → DENY

        Args:
            entity (str): The entity to validate (e.g., table name, query type).
            allowed_list (List[str]): List of allowed entities or ["*"] for wildcard.
            not_allowed_list (List[str]): List of explicitly forbidden entities.

        Returns:
            bool: True if entity is allowed, False otherwise.
        """
        # Check blacklist first (takes precedence)
        if entity in not_allowed_list:
            return False

        # Strict mode: empty allowed list means nothing is allowed
        if not allowed_list:
            return False

        # Wildcard mode: "*" allows everything (except blacklisted)
        if "*" in allowed_list:
            return True

        # Explicit whitelist check
        return entity in allowed_list

    def _is_table_allowed(self, table: str) -> bool:
        """Check if a table is allowed, handling both simple and enhanced table format."""
        # Check blacklist first
        if table in self.NOT_ALLOWED_TABLES:
            return False

        # Handle wildcard tables from our enhancement
        if hasattr(self, 'NOT_ALLOWED_TABLES') and len(self.ALLOWED_QUERIES) > 0:
            # We're using our enhanced system
            allowed_table_names = [k for k in self.ALLOWED_TABLES.keys()]
            return self._is_entity_allowed(table, allowed_table_names, self.NOT_ALLOWED_TABLES)
        else:
            # Original dev branch logic
            return table in self.ALLOWED_TABLES

    def _is_column_allowed(self, column: str) -> bool:
        """Check if a column is allowed based on column-specific rules.

        Args:
            column (str): The column name to validate.

        Returns:
            bool: True if column is allowed, False otherwise.
        """
        # Check blacklist first
        if column in self.NOT_ALLOWED_COLUMNS:
            return False

        # Strict mode: empty allowed columns means nothing is allowed
        if not self.ALLOWED_COLUMNS:
            return False

        # Wildcard mode: "*" key allows all columns
        if "*" in self.ALLOWED_COLUMNS:
            return True

        # Explicit column check
        return column in self.ALLOWED_COLUMNS

    def make_aggregate(self, aggregate: dict, param: bool = False) -> tuple[str, any]:
        """Enhanced aggregate function from dev branch."""
        # Extract the aggregate function name and its argument
        aggregate_function = list(aggregate)[0]
        aggregate_argument = aggregate[aggregate_function]

        # Construct the SQL aggregate function string
        aggregate_string = (
            f"{aggregate_function}({aggregate_argument if not param else '?'})"
        )

        # Return the aggregate string and its argument
        return aggregate_string, aggregate_argument

    def is_another_column(self, value: str) -> bool:
        """Check if a value represents another column name.

        This method determines if a value should be treated as a column reference
        rather than a literal value. With wildcard columns, we need to be more
        careful to avoid treating all strings as column references.

        Args:
            value (str): The value to check.

        Returns:
            bool: True if value is an allowed column name, False otherwise.
        """
        try:
            # If we don't have wildcard columns, use the original logic
            if "*" not in self.ALLOWED_COLUMNS:
                return self._is_column_allowed(value)

            # With wildcard columns, only treat as column if explicitly listed
            # or if it's not blacklisted and looks like a column name
            if value in self.NOT_ALLOWED_COLUMNS:
                return False

            # If we have explicit columns defined alongside wildcard, check those first
            explicit_columns = {k: v for k,
                                v in self.ALLOWED_COLUMNS.items() if k != "*"}
            if explicit_columns and value in explicit_columns:
                return True

            # For wildcard mode, we're more conservative - only treat as column
            # if it doesn't look like a typical literal value
            # This is a heuristic: values that are purely numeric, contain spaces,
            # or are common literal patterns are treated as literals
            if (value.isdigit() or
                ' ' in value or
                value.lower() in ['true', 'false', 'null'] or
                (value.startswith('"') and value.endswith('"')) or
                    (value.startswith("'") and value.endswith("'"))):
                return False

            # If we reach here with wildcard columns, treat as potential column
            return True

        except (TypeError, AttributeError):
            return False

    def is_valid_aggregate(self, aggregate: dict) -> bool:
        if not isinstance(aggregate, dict):
            return False

        operation = list(aggregate)[0]
        value = aggregate[operation]
        if operation not in self.AGGREGATES:
            return False

        return self.is_another_column(value)

    def is_valid_value(self, value: any, valuetype: any) -> bool:
        # Check if the value is an aggregate
        if isinstance(value, dict):
            return self.is_valid_aggregate(value)

        # Check if the value is another column
        if not isinstance(value, list) and self.is_another_column(value):
            return True

        # Check if the value is of the expected type
        return isinstance(value, valuetype)

    def is_special_comparison(
        self, comparator: str, value: any, valuetype: any
    ) -> bool:
        """Checks if a comparator and value match the special comparison operators.

        Special comparison operators include BETWEEN and IN. This checks if the
        comparator is one of those, and if the value matches the expected format.

        Args:
            comparator (str): The comparison operator.
            value: The comparison value.
            valuetype: The expected type of the comparison value.

        Returns:
            bool: True if it is a valid special comparison, False otherwise.
        """
        def all_values_allowed(value, valuetype):
            """Checks if all values in a list are of the specified type.

            Args:
                value (list): The list of values to check.
                valuetype: The expected type of each value.

            Returns:
                bool: True if all values match the expected type, False otherwise.
            """
            valid = True
            for entry in value:
                if not self.is_valid_value(entry, valuetype):
                    valid = False
                    break
            return valid

        if not isinstance(value, list) or not all_values_allowed(value, valuetype):
            return False

        if comparator == "BETWEEN" and len(value) == 2:
            return True

        elif comparator == "IN" and len(value) > 0:
            return True

        return False

    def is_valid_comparison(self, column: str, comparison: dict) -> bool:
        """Checks if a comparison operator and value are valid for a column.

        Validates that the comparator is a valid operator, and the value is the
        expected type for the column or a valid special comparison.

        Args:
            column (str): The column name.
            comparison (dict): The comparison operator and value.

        Returns:
            bool: True if the comparison is valid, False otherwise.
        """
        comparator = list(comparison)[0]

        if (
            comparator not in self.COMPARISON
            and comparator not in self.SPECIAL_COMPARISON
        ):
            return False

        value = comparison[comparator]
        column_type = self.ALLOWED_COLUMNS.get(
            column, self.ALLOWED_COLUMNS.get("*", object))
        if (self.is_valid_value(value, column_type) or
                self.is_special_comparison(comparator, value, column_type)):
            return True
        return False

    @staticmethod
    def get_sql_comparator(comparator: str) -> str:
        """
        Returns the SQL comparator, replacing '!=' with '<>' if necessary.

        Args:
            comparator (str): The original SQL comparator.

        Returns:
            str: The adjusted SQL comparator.
        """
        return comparator if comparator != "!=" else "<>"

    def is_value_in_allowed_categories(self, value: str) -> bool:
        """
        Checks if the given value is in the allowed categories.

        Args:
            value (str): The value to check.

        Returns:
            bool: True if the value is in the allowed categories, False otherwise.
        """
        # Enhanced logic that supports our wildcard system
        is_valid_column = self._is_column_allowed(value)
        is_logical_operator = value in self.LOGICAL
        is_comparison_operator = (value in self.SPECIAL_COMPARISON or 
                                  value in self.COMPARISON)
        
        return is_valid_column or is_logical_operator or is_comparison_operator

    def logic_parse(
        self, json_input: dict
    ) -> tuple[Literal[False], str] | tuple[Literal[True], str, tuple]:
        if len(json_input) == 0:
            return False, "Nothing To Compute"

        value: str = list(json_input.keys())[0]

        # Check if the value is in the allowed categories
        if not self.is_value_in_allowed_categories(value):
            return False, f"Invalid Input - {value}"

        elif value in self.LOGICAL and not isinstance(json_input[value], list):
            return False, f"Bad {value}, non list"

        elif value in self.ALLOWED_COLUMNS and not self.is_valid_comparison(
            value, json_input[value]
        ):
            if isinstance(json_input[value], dict):
                value0 = list(json_input[value])[0]
                if (
                    value0 not in self.COMPARISON
                    and value0 not in self.SPECIAL_COMPARISON
                ):
                    return False, f"Non Valid comparitor - {value0}"
            # Get the expected type for the column
            expected_type = self.ALLOWED_COLUMNS.get(
                value, self.ALLOWED_COLUMNS.get("*", object))
            return False, f"Bad {value}, non {expected_type}"

        if self.is_valid_comparison(value, json_input[value]):
            comparator = list(json_input[value])[0]

            adjusted_comparator = self.get_sql_comparator(comparator)

            if (
                comparator in self.COMPARISON
                and not self.is_another_column(json_input[value][comparator])
                and not isinstance(json_input[value][comparator], dict)
            ):
                return (
                    True,
                    f"{value} {adjusted_comparator} ?",
                    json_input[value][comparator]
                    if isinstance(json_input[value][comparator], tuple)
                    else (json_input[value][comparator],),
                )

            elif comparator in self.COMPARISON and self.is_another_column(
                json_input[value][comparator]
            ):
                return (
                    True,
                    f"{value} {adjusted_comparator} {json_input[value][comparator]}",
                    (),
                )

            elif list(json_input[value][comparator])[0] in self.AGGREGATES:
                # Extract the aggregate function name and its argument
                aggregate_function = list(json_input[value][comparator])[0]
                aggregate_argument = json_input[value][comparator][aggregate_function]

                return (
                    True,
                    f"{value} {adjusted_comparator} {aggregate_function}({aggregate_argument})",
                    (),
                )

            elif comparator in self.SPECIAL_COMPARISON:
                if comparator == "BETWEEN":
                    return (
                        True,
                        f"{value} BETWEEN ? AND ?",
                        tuple(json_input[value][comparator]),
                    )

                elif comparator == "IN":
                    # Determine the number of placeholders needed
                    num_placeholders = len(json_input[value][comparator])

                    # Generate the placeholders string
                    placeholders = (
                        "?"
                        if num_placeholders == 1
                        else ",".join(["?" for _ in range(num_placeholders)])
                    )

                    return (
                        True,
                        f"{value} IN ({placeholders})",
                        tuple(json_input[value][comparator]),
                    )

            return False, f"Comparitor Error - {comparator}"

        elif value in self.LOGICAL and isinstance(json_input[value], list):
            if len(json_input[value]) < 2:
                return False, "Invalid boolean length, must be >= 2"

            data = []
            safe = (True, "")
            for case in json_input[value]:
                evaluation = self.logic_parse(case)
                if not evaluation[0]:
                    safe = evaluation
                    break

                data.append(evaluation[1:])

            if not safe[0]:
                return safe

            params = []
            output = []
            for entry in data:
                if isinstance(entry[1], tuple):
                    for sub in entry[1]:
                        params.append(sub)
                else:
                    params.append(entry[1])
                output.append(entry[0])

            params = tuple(params)

            data = f"({f' {value.upper()} '.join(output)})"

            return True, data, params if isinstance(params, tuple) else (params,)

    def sql_parse(
        self, json_input: dict
    ) -> tuple[Literal[False], str] | tuple[Literal[True], str, tuple]:
        # Define the required inputs and their expected types
        required_inputs = {"query": str, "items": list, "table": str}

        # Check each required input
        for input_name, expected_type in required_inputs.items():
            # Check if the input is missing
            if input_name not in json_input:
                return False, f"Missing argument {input_name}"

            # Check if the input is of the correct type
            if not isinstance(json_input[input_name], expected_type):
                return False, f"{input_name} not right type, expected {expected_type}"

        # Validate query type using enhanced validation system
        if not self._is_entity_allowed(
            json_input["query"],
            self.ALLOWED_QUERIES,
            self.NOT_ALLOWED_QUERIES
        ):
            return False, f"Query not allowed - {json_input['query']}"

        # Validate table using enhanced validation system
        if not self._is_table_allowed(json_input["table"]):
            return False, f"Table not allowed - {json_input['table']}"

        # Enhanced item validation combining both approaches
        for index, item in enumerate(json_input["items"]):
            # Check if the item is allowed using our enhanced system first
            if not self._is_entity_allowed(
                item if isinstance(item, str) else str(item),
                self.ALLOWED_ITEMS,
                self.NOT_ALLOWED_ITEMS
            ):
                # Special case for aggregate functions (dev branch logic)
                if isinstance(item, dict) and list(item)[0] in self.AGGREGATES:
                    aggregate_function = list(item)[0]
                    aggregate_argument = item[aggregate_function]
                    
                    # Check if the aggregate argument is allowed using enhanced system
                    if not self._is_entity_allowed(
                        aggregate_argument,
                        self.ALLOWED_ITEMS,
                        self.NOT_ALLOWED_ITEMS
                    ):
                        # Fallback to dev branch table-specific logic
                        if (json_input["table"] in self.ALLOWED_TABLES and 
                            aggregate_argument in self.ALLOWED_TABLES[json_input["table"]]):
                            # Update the item with the aggregate function
                            json_input["items"][index] = (
                                f"{aggregate_function}({aggregate_argument})"
                            )
                        else:
                            return False, f"Item not allowed - {aggregate_argument}"
                    else:
                        # Update the item with the aggregate function
                        json_input["items"][index] = (
                            f"{aggregate_function}({aggregate_argument})"
                        )
                else:
                    # Fallback to dev branch table-specific logic for non-aggregates
                    if (json_input["table"] in self.ALLOWED_TABLES and 
                        item in self.ALLOWED_TABLES[json_input["table"]]):
                        continue  # Item is allowed
                    else:
                        return False, f"Item not allowed - {item}"

        # Validate connection using enhanced validation system
        if "connection" in json_input and not self._is_entity_allowed(
            json_input["connection"],
            self.ALLOWED_CONNECTIONS,
            self.NOT_ALLOWED_CONNECTIONS
        ):
            return False, f"Connection not allowed - {json_input['connection']}"

        sql_string = f"{json_input['query']} {','.join(json_input['items'])} FROM {json_input['table']}"

        if "logic" in json_input:
            logic_string = self.logic_parse(json_input["logic"])
            if not logic_string[0]:
                return False, f"Logic Fail - {logic_string[1]}"

            return (
                True,
                f"{sql_string} {json_input['connection']} {logic_string[1]}",
                logic_string[2]
                if isinstance(logic_string[2], tuple)
                else (logic_string[2],),
            )

        return True, sql_string, ()