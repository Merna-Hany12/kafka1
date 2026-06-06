import duckdb

con = duckdb.connect()

query = """
SELECT
    location,
    COUNT(*) AS total_transactions,
    ROUND(AVG(amount), 2) AS avg_amount,
    MAX(amount) AS max_amount,
    MIN(amount) AS min_amount
FROM read_parquet('data_lake/*.parquet')
WHERE amount IS NOT NULL
  AND location IS NOT NULL
GROUP BY location
ORDER BY total_transactions DESC
"""

result = con.execute(query).df()

print(result)