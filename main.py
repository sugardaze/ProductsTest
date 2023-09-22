import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ProductsTest").getOrCreate()

products_data = [(1, "Product A"), (2, "Product B"), (3, "Product C")]
categories_data = [(1, "Category 1"), (2, "Category 2")]

products_df = spark.createDataFrame(products_data, ["ProductID", "ProductName"])
categories_df = spark.createDataFrame(categories_data, ["CategoryID", "CategoryName"])

products_df.createOrReplaceTempView("products")
categories_df.createOrReplaceTempView("categories")

result_df = spark.sql("""
    SELECT p.ProductName, COALESCE(c.CategoryName, 'No Category') as CategoryName
    FROM products p
    LEFT JOIN categories c ON p.ProductID = c.CategoryID
""")

result_df.show()