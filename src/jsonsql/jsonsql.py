from typing import Literal


class JsonSQL:
    def __init__(
        self,
        allowed_queries: list = [],
        allowed_items: list = [],
        allowed_tables: list = [],
        allowed_connections: list = [],
        allowed_columns: dict[str:type] = {},
    ):
        """Initializes JsonSQL instance with allowed queries, items, tables,
        connections, and columns.

        Args:
        allowed_queries (list): Allowed SQL query strings.
        allowed_items (list): Allowed SQL SELECT fields.
        allowed_tables (list): Allowed SQL FROM tables.
        allowed_connections (list): Allowed SQL JOIN conditions.
        allowed_columns (dict): Allowed columns per table.
        """
        table_dict = {}
        for table in allowed_tables:
            if isinstance(table, dict) and isinstance(table[list(table)[0]], list):
                table_dict[list(table)[0]] = table[list(table)[0]]
            elif isinstance(table, dict) and not isinstance(
                allowed_tables[table], list
            ):
                raise TypeError(f"Table {table} items must be a list")
            elif isinstance(table, str):
                table_dict[table] = [None]
            else:
                raise TypeError(f"{table} not str or dict")

        self.ALLOWED_QUERIES = allowed_queries
        self.ALLOWED_ITEMS = allowed_items
        self.ALLOWED_TABLES = table_dict
        self.ALLOWED_CONNECTIONS = allowed_connections
        self.ALLOWED_COLUMNS = allowed_columns

        self.LOGICAL = ("AND", "OR")
        self.COMPARISON = ("=", ">", "<", ">=", "<=", "<>", "!=")
        self.SPECIAL_COMPARISON = ("BETWEEN", "IN")
        self.AGGREGATES = ("MIN", "MAX", "SUM", "AVG", "COUNT")

    def make_aggregate(self, aggregate: dict, param: bool = False) -> tuple[str, any]:
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
        try:
            return value in self.ALLOWED_COLUMNS
        except TypeError:
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
        if self.is_valid_value(
            value, self.ALLOWED_COLUMNS[column]
        ) or self.is_special_comparison(
            comparator, value, self.ALLOWED_COLUMNS[column]
        ):
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
        return (
            value in self.ALLOWED_COLUMNS
            or value in self.LOGICAL
            or value in self.SPECIAL_COMPARISON
            or value in self.COMPARISON
        )

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
            return False, f"Bad {value}, non {self.ALLOWED_COLUMNS[value]}"

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

        if json_input["query"] not in self.ALLOWED_QUERIES:
            return False, f"Query not allowed - {json_input['query']}"

        if json_input["table"] not in self.ALLOWED_TABLES:
            return False, f"Table not allowed - {json_input['table']}"

        # Iterate over each item in the "items" list
        for index, item in enumerate(json_input["items"]):
            # Check if the item is allowed
            if (
                item not in self.ALLOWED_ITEMS
                and item not in self.ALLOWED_TABLES[json_input["table"]]
            ):
                # Special case for aggregate functions
                if isinstance(item, dict) and list(item)[0] in self.AGGREGATES:
                    aggregate_function = list(item)[0]
                    aggregate_argument = item[aggregate_function]
                    # Check if the aggregate argument is allowed
                    if (
                        aggregate_argument in self.ALLOWED_ITEMS
                        or aggregate_argument
                        in self.ALLOWED_TABLES[json_input["table"]]
                    ):
                        # Update the item with the aggregate function
                        json_input["items"][index] = (
                            f"{aggregate_function}({aggregate_argument})"
                        )
                    else:
                        return False, f"Item not allowed - {aggregate_argument}"
                else:
                    return False, f"Item not allowed - {item}"

        if (
            "connection" in json_input
            and json_input["connection"] not in self.ALLOWED_CONNECTIONS
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
