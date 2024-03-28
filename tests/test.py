from jsonsql import JsonSQL

if __name__ == "__main__":
    request = {
        "query": "SELECT",
        "items": ["*"],
        "table": "images",
        "connection": "WHERE",
        "logic": {
            "AND": [
                {"creature": "owlbear"},
                {"OR": [
                    {"userID": 555},
                    {"userID": 111}
                ]}
            ]}
    }

    allowed_queries = [
        "SELECT"
    ]

    allowed_items = [
        "*"
    ]

    allowed_connections = [
        "WHERE"
    ]

    allowed_tables = [
        "images"
    ]

    allowed_columns = {
        "creature": str,
        "userID": int
    }

    jsonsql_ = JsonSQL(allowed_queries, allowed_items,
                       allowed_tables, allowed_connections, allowed_columns)

    print(jsonsql_.sql_parse(request))