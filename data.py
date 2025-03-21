from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession.builder.appName("customer_analysis").getOrCreate()

    print("Reading dataset.csv ... ")
    path_customers = "dataset.csv"
    df_customers = spark.read.csv(path_customers, header=True, inferSchema=True)

    df_customers.createOrReplaceTempView("customers")

    # Mostrar estructura de los datos
    query = "DESCRIBE customers"
    spark.sql(query).show(20)

    # Top 10 clientes que más gastaron
    query = """
        SELECT Customer_ID, SUM(Purchase_Amount) AS Total_Spending
        FROM customers
        GROUP BY Customer_ID
        ORDER BY Total_Spending DESC
        LIMIT 10
    """
    df_top_customers = spark.sql(query)
    df_top_customers.show()

    # Cantidad promedio de compras en base a edad
    query = """
        SELECT Age, AVG(Purchase_Amount) AS Avg_Spending
        FROM customers
        GROUP BY Age
        ORDER BY Age
    """
    df_age_spending = spark.sql(query)
    df_age_spending.show()

    # Categorías más compradas
    query = """
        SELECT Purchase_Category, COUNT(*) AS Purchase_Count
        FROM customers
        GROUP BY Purchase_Category
        ORDER BY Purchase_Count DESC
    """
    df_top_categories = spark.sql(query)
    df_top_categories.show()

    results = df_top_categories.toJSON().collect()
    with open("results/top_categories.json", "w") as file:
        json.dump(results, file)

    spark.stop()