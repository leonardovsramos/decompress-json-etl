# Processing Highly Compressed JSON at Scale on AWS 🚀

Imagine dealing with a production OLTP database with no indexing, poor performance, and billions of records in the main business tables. Sounds like a nightmare for any data engineer, right? Recently, we faced exactly this challenge in one of the projects where we worked as a Data Engineer.

## 🎯 The Challenge:
Extract information from this source database and make it available to users within AWS, despite the sluggish performance and massive size of the tables.

## 💡 The Insight:
During our investigation, we discovered a column that stored the same information as the tables with billions of records but in JSON format compressed with **GZIP**. While optimized for storage, this column contained records with more than **800,000 characters each**!

## 🔧 The Strategic Solution:
We decided to ingest only this compressed column into **S3**, process it, and decompress it using **AWS Glue** with **PySpark**. The data was transformed into multiple tables organized in the **Iceberg** format, optimizing consumption through **AWS Athena**. Additionally, we created summarized views in the **Data Warehouse**, making the data more accessible and analysis-ready.

## 🚀 Exploring Different Approaches:
We tested decompression and transformation using:
- **Pandas + PySpark UDFs**: efficient but with the overhead of transforming between Python and the JVM.
- **Scala**: eliminated the Python → JVM transformation, resulting in better performance.

## 💾 Simulation and Reproducibility:
We created a GitHub repository to simulate the database behavior by generating Parquet files that replicate the original structure. From there, we processed the data and made it available in **Iceberg** tables using a Hadoop catalog.

This experience reinforced something we always believe: even in adverse scenarios, with detailed analysis it is possible to find robust solutions for complex data processing challenges.

If you’ve faced similar challenges, share your experiences in the comments! Let’s exchange ideas and insights! 🚀💬

## 📖 Step by step

### 1. UDF
Here we show the UDF code in Scala and the manual compilation of the JAR file. This section is optional and you can use the pre-compiled
file available in the repository.

#### 1.1 Scala code

PySpark needs a JAR file containing our UDF in Scala in order to run it. So, starting with a basic Scala
project, create a file called *Decompress.scala* containing the following code:

  ```scala
  import org.apache.spark.sql.api.java.UDF1
  import java.io.ByteArrayInputStream
  import java.util.zip.GZIPInputStream

  class Decompress extends UDF1[Array[Byte], Array[Byte]] {
    override def call(t1: Array[Byte]): Array[Byte] = {
      val inputStream = new GZIPInputStream(new ByteArrayInputStream(t1))
      org.apache.commons.io.IOUtils.toByteArray(inputStream)
    }
  }
  ```

Note that our Decompress class is extending Spark's UDF1 which accepts one argument, in this case
an array of bytes (compressed data) and we also expect the output to be an array of bytes (uncompressed data).

Now that we have our code complete, we can compile it with sbt:

`sbt clean package`

The necessary JAR file can be found in the target folder of your project.

#### 1.2 Python code

To allow PySpark to recognize our UDF, we need to add the JAR file when we create our Spark session:

```python
spark = (SparkSession.builder
.config(“spark.jars”, “./scala_udf.jar”)
.getOrCreate())
```
Now let's register it in the SQL context so we can call it with Spark SQL:

```python
sqlContext = SQLContext(spark.sparkContext)
spark.udf.registerJavaFunction(“decompress_scala”, “Decompress”, T.BinaryType())
```
Here decompress_scala will be the name registered in the SQL context, Decompress is the name of our UDF class and BinaryType
refers to its return type.

To use it, just add the following line of code:
```python
df = spark.sql(“select *, decode(decompress_scala(compressed_json), ‘utf8’) as json from df”)
```
Here, we used decode to convert our binary output into a utf-8 string, as this was necessary in the chosen example.

#### 1.3 Execution results
Reading the provided data.parquet file, we can decompress and decode the gzipped JSON column:

![Decompress result](./img.png)

### 2. Main code
#### 2.1 Features

- Generation of dummy datasets for customers, orders and payments using `Faker`.
- Storing data in compressed JSON format within Parquet files.
- Decompressing and transforming JSON data into structured tables with PySpark.
- Loading processed data into Iceberg tables with predefined schemas.
- Support for imposing schemas and scalable ETL workflows.

#### 2.2 Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/leonardovsramos/decompress-json-etl.git
   ```

2. Navigate to the project directory:
   ```bash
   cd decompress-json-etl
   ```

3. Install the dependencies using `uv`:
   ```bash
   uv setup
   ```

---

#### 2.3 Usage

##### 2.3.1 Generating dummy data
Run the script to generate compressed JSON data:
```bash
python generate_json.py --num_orders <order_number> --output_path <output_file_path>
```

**Example:**
```bash
python generate_json.py --num_orders 100 --output_path ./data
```

##### 2.3.2 Processing data in Iceberg tables
Use the ETL script to unpack and load the data into Iceberg tables:
```bash
python process_json.py --input_path <input_file_path>
```

**Example:**
```bash
python process_json.py --input_path ./data/compressed_json.parquet
```

---

#### 2.4 How it works

##### 2.4.1 Data generation
- Fictitious data for customers, orders and payments is created using the `Faker` library.
- The data is compressed into JSON format and stored in Parquet files.

##### 2.4.2 Data transformation
- The compressed JSON is decompressed using a UDF in Scala.
- PySpark transforms the JSON data into structured records for:
    - Orders
    - Customers
    - Order items
    - Payments

##### 2.4.3 Loading data
- The transformed data is loaded into Iceberg tables, applying predefined schemas.

#### 2.5 Schema definitions

##### 2.5.1 Orders table
| Column | Type | Description                     |
|---------------|:------------------:|---------------------------------|
| order_id | INT | Unique identifier of the order. |
| order_date | TIMESTAMP | Order timestamp.                |
| order_price | DECIMAL(15,2) | Total order price.              |

##### 2.5.2 Clients table
| Column | Type | Description                       |
|---------------|:------------------:|-----------------------------------|
| order_id | INT | Identifier linked to the order.   |
| client_id | INT | Unique client identifier.         |
| first_name | STRING | Customer's first name.            |
| last_name | STRING | Customer's last name.             |
| cpf | STRING | Customer's Brazilian CPF.         |
| rg | STRING | customer's Brazilian Personal ID. |
| email | STRING | Customer's email address.         |
| birth_date | DATE | Customer's date of birth.         |

##### 2.5.3 Order Items table
| Column | Type | Description                     |
|------------------|:------------------:|---------------------------------|
| order_id | INT | Identifier linked to the order. |
| id | INT | Unique identifier of the item.  |
| item_name | STRING | Item's name.                    |
| item_price | DECIMAL(15,2) | Item's price.                   |
| item_description | STRING | Item's description.             |

##### 2.5.4 Payments table
| Column | Type | Description |
|---------------------------|:------------------:|-----------------------------|
| order_id | INT | Identifier linked to the order. |
| id | INT | Unique identifier of the payment. |
| method | STRING | Payment method (cash, credit card, etc.). |
| amount | DECIMAL(15,2) | Total amount paid. |
| tranch_value | DECIMAL(15,2) | Amount of each installment (if applicable). |
| tranch_payment_date | DATE | Installment payment date. |
| tranch_installment_number | SMALLINT | Installment number. |

## Repository on GitHub
https://github.com/leonardovsramos/decompress-json-etl

## References
https://spark.apache.org/docs/3.5.1/api/java/org/apache/spark/sql/api/java/UDF1.html
https://spark.apache.org/docs/3.5.2/sql-ref-functions-udf-scalar.html

## Authors
[Leonardo Vieira dos Santos Ramos](https://www.linkedin.com/in/leonardolvsr/)

[Vítor Rodrigues Gôngora](https://www.linkedin.com/in/vitorgongora/)