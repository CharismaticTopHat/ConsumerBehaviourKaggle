from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession.builder.appName("videogame_analysis").getOrCreate()

    path_videogames = "dataset.csv"
    df_videogames = spark.read.csv(path_videogames, header=True, inferSchema=True)

    df_videogames.createOrReplaceTempView("videogames")

    # Top 10 juegos más caros
    query = """
        SELECT name, price
        FROM videogames
        ORDER BY price DESC
        LIMIT 10
    """
    df_top_expensive = spark.sql(query)
    df_top_expensive.show()
    results = df_top_expensive.toJSON().collect()
    with open("results/top_expensive_games.json", "w") as file:
        json.dump(results, file)

    # Tiempo de juego promedio
    query = """
        SELECT AVG(hltb_single) AS avg_playtime
        FROM videogames
        WHERE hltb_single IS NOT NULL
        LIMIT 100
    """
    df_avg_playtime = spark.sql(query)
    df_avg_playtime.show()
    results = df_avg_playtime.toJSON().collect()
    with open("results/avg_playtime.json", "w") as file:
        json.dump(results, file)

    # Top 10 juegos con la más opiniones poisitivas que negativas en relación
    query = """
        SELECT name, positive, negative,
               (positive * 1.0 / (positive + negative)) AS review_ratio
        FROM videogames
        WHERE (positive + negative) > 0
        ORDER BY review_ratio DESC
        LIMIT 10
    """
    df_best_reviews = spark.sql(query)
    df_best_reviews.show()
    results = df_best_reviews.toJSON().collect()
    with open("results/top_reviewed_games.json", "w") as file:
        json.dump(results, file)

    spark.stop()
