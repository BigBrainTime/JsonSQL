from jsonsql import JsonSQL

request = {
    "query": "SELECT",
    "items": ["*"],
    "table": "images",
    "connection": "WHERE",
    "logic": {
        "AND": [
            {"creature": {"=": "owlbear"}},
            {"OR": [
                {"userID": {"<=": 555}},
                {"userID": {"=": 111}},
                {"AND": [
                    {"userID": {"=": 1111}},
                    {"imageID": {"=": "imageIDString"}}
                ]},
                {"userID": {"BETWEEN": [1000, 2000]}}
            ]},
        ]}
}

request2 = {
    "AND": [
        {"creature": {"=": "owlbear"}},
        {"ranking": {">": 0}},
        {"ranking": {"=": "votes"}},
    ]
}

request3 = {"creature": {"=": "owlbear"}}

request4 = {
    "query": "SELECT",
    "items": [{"COUNT": "userID"}],
    "table": "images",
    "connection": "WHERE",
    "logic": {"userID": {"=": {"MIN":"userID"}}},
}

allowed_queries = [
    "SELECT"
]

allowed_items = [
    "*",
    "creature"
]

allowed_connections = [
    "WHERE"
]

allowed_tables = [
    {"images":["userID"]}
]

allowed_columns = {
    "creature": str,
    "userID": int,
    "imageID":str,
    "ranking":int,
    "votes":int
}

jsonsql_ = JsonSQL(allowed_queries, allowed_items,allowed_tables, allowed_connections, allowed_columns)

print(jsonsql_.sql_parse(request))
print(jsonsql_.logic_parse(request2))
print(jsonsql_.logic_parse(request3))
print(jsonsql_.sql_parse(request4))
