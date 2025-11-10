from nanoquery.executor import QueryExecutor

SQL = """SELECT s.song_id, AVG(u.age) AS avg_age,
COUNT(DISTINCT l.user_id)
FROM Songs s
JOIN Listens l ON s.song_id = l.song_id
JOIN Users u ON l.user_id = u.user_id
GROUP BY s.song_id, s.title
ORDER BY COUNT(DISTINCT l.user_id) DESC, s.song_id;"""

if __name__ == "__main__":
    paths = {
        "Songs": "data/Songs.parquet",
        "Listens": "data/Listens.parquet",
        "Users": "data/Users.parquet",
    }
    executor = QueryExecutor(paths, working_dir="temp")
    df, plan = executor.execute(SQL)
    print("Plan:", plan["steps"])
    print(df.head(10))
    df.to_csv("result.csv", index=False)
    print("Saved result.csv")
