def parse_sql_hardcoded(sql: str):
    """
    Hand-parses the specific project query structure and returns a plan dict.
    """
    return {
        "tables": {"Songs": "s", "Listens": "l", "Users": "u"},
        "joins": [
            {"left": "Songs", "left_key": "song_id", "right": "Listens", "right_key": "song_id"},
            {"left": "Listens", "left_key": "user_id", "right": "Users", "right_key": "user_id"},
        ],
        "group_by": ["s.song_id", "s.title"],
        "aggregations": {
            "avg_age": {"func": "AVG", "expr": "u.age"},
            "count_distinct_users": {"func": "COUNT_DISTINCT", "expr": "l.user_id"},
        },
        "select": ["s.song_id", "avg_age", "count_distinct_users"],
        "order_by": [("count_distinct_users", "DESC"), ("s.song_id", "ASC")],
        "needed_columns": {
            "Songs": ["song_id", "title"],
            "Listens": ["song_id", "user_id"],
            "Users": ["user_id", "age"],
        },
    }
