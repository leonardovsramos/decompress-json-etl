import argparse

import pyspark.sql.functions as F  # type: ignore
import pyspark.sql.types as T  # type: ignore
from pyspark.sql import DataFrame, SparkSession  # type: ignore

TABLE_DEFINITIONS: dict[str : dict[str:str]] = {
    "local.default.order": {
        "schema": """
            order_id INT,
            order_date TIMESTAMP,
            order_price DECIMAL(15,2)
        """,
        "properties": {"format_version": "2"},
    },
    "local.default.client": {
        "schema": """
            order_id INT,
            client_id INT,
            first_name STRING,
            last_name STRING,
            cpf STRING,
            rg STRING,
            email STRING,
            birth_date DATE
        """,
        "properties": {"format-version": "2"},
    },
    "local.default.order_items": {
        "schema": """
            order_id INT,
            id INT,
            item_name STRING,
            item_price DECIMAL(15,2),
            item_description STRING
        """,
        "properties": {"format-version": "2"},
    },
    "local.default.payment": {
        "schema": """
            order_id INT,
            id INT,
            method STRING,
            amount DECIMAL(15,2),
            tranch_value DECIMAL(15,2),
            tranch_payment_date DATE,
            tranch_instalslment_number SMALLINT
        """,
        "properties": {"format-version": "2"},
    },
}


def create_iceberg_tables(spark: SparkSession) -> None:
    for table_name, table_config in TABLE_DEFINITIONS.items():
        # Drop table if it exists
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

        # Create the table with specific schema and properties
        schema = table_config["schema"]
        properties = table_config["properties"]
        properties_clause = ", ".join(
            f"'{key}' = '{value}'" for key, value in properties.items()
        )

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            (
                {schema}
            )
            USING ICEBERG
            TBLPROPERTIES ({properties_clause})
            """
        )


def insert_into_table(df: DataFrame, table_name: str, spark: SparkSession) -> None:
    df.createOrReplaceTempView("temp_view")
    spark.sql(f"INSERT INTO {table_name} SELECT * FROM temp_view")


def order_etl(df: DataFrame, spark: SparkSession) -> None:
    order_df = df.select(
        F.col("json.order_id"),
        F.to_timestamp(F.col("json.order_date"), format="yyyy-MM-dd HH:mm:ss").alias(
            "order_date"
        ),
        F.col("json.order_price"),
    )

    insert_into_table(order_df, "local.default.order", spark)


def client_etl(df: DataFrame, spark: SparkSession) -> None:
    client_df = df.select(
        F.col("json.order_id"),
        F.col("json.client.id").alias("client_id"),
        F.col("json.client.first_name").alias("first_name"),
        F.col("json.client.last_name").alias("last_name"),
        F.col("json.client.cpf").alias("cpf"),
        F.col("json.client.rg").alias("rg"),
        F.col("json.client.email").alias("email"),
        F.to_date(F.col("json.client.birth_date"), format="yyyy-MM-dd").alias(
            "birth_date"
        ),
    )

    insert_into_table(client_df, "local.default.client", spark)


def order_items_etl(df: DataFrame, spark: SparkSession) -> None:
    order_items_df = df.select(
        F.col("json.order_id"), F.explode(F.col("json.order_items")).alias("order_item")
    ).select(
        F.col("order_id"),
        F.col("order_item.id"),
        F.col("order_item.item_name"),
        F.col("order_item.item_price"),
        F.col("order_item.item_description"),
    )

    insert_into_table(order_items_df, "local.default.order_items", spark)


def payment_etl(df: DataFrame, spark: SparkSession) -> None:
    payment_df = df.select(
        F.col("json.order_id"),
        F.col("json.payment.id"),
        F.col("json.payment.method"),
        F.col("json.payment.amount"),
        F.col("json.payment.installments"),
        F.explode_outer(F.col("json.payment.tranches")).alias("tranch"),
    ).select(
        F.col("order_id"),
        F.col("id"),
        F.col("method"),
        F.col("amount"),
        F.col("tranch.value").alias("tranch_value"),
        F.to_date(
            F.col("tranch.date_of_payment"),
            format="yyyy-MM-dd",
        ).alias("tranch_payment_date"),
        F.col("tranch.installment_number").alias("tranch_instalslment_number"),
    )

    insert_into_table(payment_df, "local.default.payment", spark)


def main(input_path: str) -> None:
    # Initialize Spark session with Iceberg configurations
    spark = (
        SparkSession.builder.appName("IcebergLocalDevelopment")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        .config(
            "spark.sql.catalog.spark_catalog.type",
            "hive",
        )
        .config(
            "spark.sql.catalog.local",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            "spark.sql.catalog.local.type",
            "hadoop",
        )
        .config(
            "spark.sql.catalog.local.warehouse",
            "warehouse",
        )
        .config(
            "spark.jars",
            "./jar/scala_udf.jar",
        )
        .getOrCreate()
    )

    spark.udf.registerJavaFunction("decompress_scala", "Decompress", T.BinaryType())

    # Create iceberg tables
    create_iceberg_tables(spark)

    json_schema = T.StructType(
        [
            T.StructField("order_id", T.IntegerType(), True),
            T.StructField("order_date", T.StringType(), True),
            T.StructField("order_price", T.DecimalType(15, 2), True),
            T.StructField(
                "client",
                T.StructType(
                    [
                        T.StructField("id", T.IntegerType(), True),
                        T.StructField("first_name", T.StringType(), True),
                        T.StructField("last_name", T.StringType(), True),
                        T.StructField("cpf", T.StringType(), True),
                        T.StructField("rg", T.StringType(), True),
                        T.StructField("email", T.StringType(), True),
                        T.StructField("birth_date", T.StringType(), True),
                    ]
                ),
                True,
            ),
            T.StructField(
                "order_items",
                T.ArrayType(
                    T.StructType(
                        [
                            T.StructField("id", T.IntegerType(), True),
                            T.StructField("item_name", T.StringType(), True),
                            T.StructField("item_price", T.DecimalType(15, 2), True),
                            T.StructField("item_description", T.StringType(), True),
                        ]
                    )
                ),
                True,
            ),
            T.StructField(
                "payment",
                T.StructType(
                    [
                        T.StructField("id", T.IntegerType(), True),
                        T.StructField("method", T.StringType(), True),
                        T.StructField("amount", T.DecimalType(15, 2), True),
                        T.StructField("installments", T.IntegerType(), True),
                        T.StructField(
                            "tranches",
                            T.ArrayType(
                                T.StructType(
                                    [
                                        T.StructField(
                                            "value", T.DecimalType(15, 2), True
                                        ),
                                        T.StructField(
                                            "date_of_payment", T.StringType(), True
                                        ),
                                        T.StructField(
                                            "installment_number", T.IntegerType(), True
                                        ),
                                    ]
                                )
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
        ]
    )

    etl_functions = {
        "order": order_etl,
        "client": client_etl,
        "order_items": order_items_etl,
        "payment": payment_etl,
    }

    df = spark.read.load(input_path)

    df = df.withColumn(
        "json", F.expr("decode(decompress_scala(compressed_json), 'utf8')")
    ).withColumn("json", F.from_json(F.col("json"), json_schema))

    for table_name, etl_function in etl_functions.items():
        try:
            etl_function(df, spark)
        except Exception as e:
            print(f"An error occured while processing the table `{table_name}`")
            raise e


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "This script processes a Parquet file containing compressed JSON data, "
            "decompresses it using a Scala UDF, and transforms the data into multiple "
            "Iceberg tables (`order`, `client`, `order_items`, and `payment`) in a "
            "Hadoop-backed Iceberg catalog. The script expects the Parquet file to "
            "contain a `compressed_json` column, which is decompressed and parsed using "
            "a predefined JSON schema."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--input_path",
        type=str,
    )

    args = parser.parse_args()
    input_path = args.input_path

    main(input_path)
