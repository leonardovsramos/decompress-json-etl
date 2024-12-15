
# decompress-json-etl

A Python-based ETL pipeline for processing compressed JSON data into structured Iceberg tables. This project supports synthetic data generation, efficient storage, and scalable data processing with PySpark.

---

## Features

- Generate synthetic datasets for clients, orders, and payments using `Faker`.
- Store data in compressed JSON format within Parquet files.
- Decompress and transform JSON data into structured tables with PySpark.
- Load processed data into Iceberg tables with predefined schemas.
- Supports schema enforcement and scalable ETL workflows.

---

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-repo/decompress-json-etl.git
   ```

2. Navigate to the project directory:
   ```bash
   cd decompress-json-etl
   ```

3. Install dependencies using `uv`:
   ```bash
   uv setup
   ```

4. (OPTIONAL) Compile the Scala UDF
If you don't want to use the pre-compiled jar, it can be compiled with:
   ```bash
   sbt clean package
   ```

---

## Usage

### 1. Generate Synthetic Data
Run the script to generate compressed JSON data:
```bash
python generate_json.py --num_orders <number_of_orders> --output_path <output_file_path>
```

**Example:**
```bash
python generate_json.py --num_orders 100 --output_path ./data
```

### 2. Process Data into Iceberg Tables
Use the ETL script to decompress and load data into Iceberg tables:
```bash
python process_json.py --input_path <input file path>
```

**Example:**
```bash
python process_json.py --input_path ./data/compressed_json.parquet
```

---

## How It Works

### Data Generation
- Synthetic data for clients, orders, and payments is created using the `Faker` library.
- Data is compressed into JSON format and stored in Parquet files.

### Data Transformation
- Compressed JSON is decompressed using a Scala UDF.
- PySpark transforms the JSON data into structured records for:
  - Orders
  - Clients
  - Order Items
  - Payments

### Data Loading
- Transformed data is loaded into Iceberg tables, enforcing predefined schemas and properties.

---

## Schema Definitions

### Orders Table
| Column       | Type               | Description           |
|--------------|:------------------:|-----------------------|
| order_id     | INT                | Unique identifier for the order. |
| order_date   | TIMESTAMP          | Date and time of the order. |
| order_price  | DECIMAL(15,2)      | Total price of the order. |

### Clients Table
| Column       | Type               | Description           |
|--------------|:------------------:|-----------------------|
| order_id     | INT               | Identifier linking to the order. |
| client_id    | INT               | Unique identifier for the client. |
| first_name   | STRING            | Client's first name. |
| last_name    | STRING            | Client's last name. |
| cpf          | STRING            | Brazilian CPF identifier. |
| rg           | STRING            | Brazilian RG identifier. |
| email        | STRING            | Client's email address. |
| birth_date   | DATE              | Client's date of birth. |

### Order Items Table
| Column            | Type              | Description           |
|-------------------|:-----------------:|-----------------------|
| order_id          | INT               | Identifier linking to the order. |
| id                | INT               | Unique identifier for the item. |
| item_name         | STRING            | Name of the item. |
| item_price        | DECIMAL(15,2)     | Price of the item. |
| item_description  | STRING            | Description of the item. |

### Payments Table
| Column                     | Type              | Description           |
|----------------------------|:-----------------:|-----------------------|
| order_id                   | INT               | Identifier linking to the order. |
| id                         | INT               | Unique identifier for the payment. |
| method                     | STRING            | Payment method (cash, credit card, etc.). |
| amount                     | DECIMAL(15,2)     | Total amount paid. |
| tranch_value               | DECIMAL(15,2)     | Value of each installment (if applicable). |
| tranch_payment_date        | DATE              | Date of the installment payment. |
| tranch_installment_number  | SMALLINT          | Installment number. |

---

## Requirements

- Python 3.13 or higher
- PySpark
- Iceberg runtime JARs
- Hadoop environment
- Scala UDF (decompression function)

---

## License

This project is licensed under the MIT License. See [LICENSE](./LICENSE) for more details.

---

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.

---

## Contact

For questions or support, please contact [leonardovsr.dev@gmail.com].