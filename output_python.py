import pandas as pd
import sqlite3

try:
    conn = sqlite3.connect('Data Engineer_ETL Assignment.db')

    sql_query = """
    SELECT c.Customer_id, c.Age, i.Item_name, SUM(CAST(o.Quantity AS INTEGER)) AS Total_Quantity
    FROM Customers c
    JOIN Sales s ON c.Customer_id = s.Customer_id
    JOIN Orders o ON s.Sales_id = o.Sales_id
    JOIN Items i ON o.Item_id = i.Item_id
    WHERE c.Age BETWEEN 18 AND 35
    GROUP BY c.Customer_id, i.Item_name
    HAVING SUM(CAST(o.Quantity AS INTEGER)) > 0;
    """

    df = pd.read_sql_query(sql_query, conn)

    df.to_csv('output_file.csv', index=False, sep=';', columns=['Customer_id', 'Age', 'Item_name', 'Total_Quantity'])

    print("Data successfully exported to output_file.csv")

except sqlite3.Error as e:
    print("SQLite error:", e)

except Exception as e:
    print("An error occurred:", e)

finally:
    if conn:
        conn.close()
