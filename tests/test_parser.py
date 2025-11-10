from nanoquery.parser import parse_sql_hardcoded

def test_parse_sql_hardcoded():
    plan = parse_sql_hardcoded("ignored")
    assert "joins" in plan and len(plan["joins"]) == 2
    assert plan["needed_columns"]["Listens"] == ["song_id", "user_id"]
