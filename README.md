
# Pipeline ETL em Python: Transformando JSON Compactado em Tabelas Iceberg

Uma pipeline ETL baseado em Python para processar dados JSON compactados em tabelas estruturadas no Iceberg. Este projeto suporta a geração de dados fictícios, armazenamento eficiente e processamento escalável de dados com PySpark.

---

## Recursos

- Geração de conjuntos de dados fictícios para clientes, pedidos e pagamentos usando `Faker`.
- Armazenamento de dados no formato JSON compactado dentro de arquivos Parquet.
- Descompactação e transformação de dados JSON em tabelas estruturadas com PySpark.
- Carregamento dos dados processados em tabelas Iceberg com esquemas predefinidos.
- Suporte à imposição de esquemas e fluxos de ETL escaláveis.

---

## Instalação

1. Clone o repositório:
   ```bash
   git clone https://github.com/your-repo/decompress-json-etl.git
   ```

2. Navegue até o diretório do projeto:
   ```bash
   cd decompress-json-etl
   ```

3. Instale as dependências usando `uv`:
   ```bash
   uv setup
   ```

---

## Uso

### 1. Gerar Dados Fictícios
Execute o script para gerar dados JSON compactados:
```bash
python generate_json.py --num_orders <numero_de_pedidos> --output_path <caminho_do_arquivo_de_saida>
```

**Exemplo:**
```bash
python generate_json.py --num_orders 100 --output_path ./data
```

### 2. Processar Dados em Tabelas Iceberg
Use o script ETL para descompactar e carregar os dados nas tabelas Iceberg:
```bash
python process_json.py --input_path <caminho_do_arquivo_de_entrada>
```

**Exemplo:**
```bash
python process_json.py --input_path ./data/compressed_json.parquet
```

---

## Como Funciona

### Geração de Dados
- Dados fictícios para clientes, pedidos e pagamentos são criados usando a biblioteca `Faker`.
- Os dados são compactados no formato JSON e armazenados em arquivos Parquet.

### Transformação de Dados
- O JSON compactado é descompactado usando uma UDF em Scala.
- O PySpark transforma os dados JSON em registros estruturados para:
  - Pedidos
  - Clientes
  - Itens do Pedido
  - Pagamentos

### Carregamento de Dados
- Os dados transformados são carregados em tabelas Iceberg, aplicando esquemas e propriedades predefinidos.

---

## Definições de Esquema

### Tabela de Pedidos (Orders)
| Coluna        | Tipo               | Descrição                    |
|---------------|:------------------:|-----------------------------|
| order_id      | INT                | Identificador único do pedido. |
| order_date    | TIMESTAMP          | Data e hora do pedido.      |
| order_price   | DECIMAL(15,2)      | Preço total do pedido.      |

### Tabela de Clientes (Clients)
| Coluna        | Tipo               | Descrição                   |
|---------------|:------------------:|-----------------------------|
| order_id      | INT               | Identificador vinculado ao pedido. |
| client_id     | INT               | Identificador único do cliente. |
| first_name    | STRING            | Primeiro nome do cliente.  |
| last_name     | STRING            | Sobrenome do cliente.      |
| cpf           | STRING            | CPF brasileiro do cliente. |
| rg            | STRING            | RG brasileiro do cliente.  |
| email         | STRING            | Endereço de e-mail do cliente. |
| birth_date    | DATE              | Data de nascimento do cliente. |

### Tabela de Itens do Pedido (Order Items)
| Coluna           | Tipo               | Descrição                   |
|------------------|:------------------:|-----------------------------|
| order_id         | INT                | Identificador vinculado ao pedido. |
| id               | INT                | Identificador único do item. |
| item_name        | STRING             | Nome do item.              |
| item_price       | DECIMAL(15,2)      | Preço do item.             |
| item_description | STRING             | Descrição do item.         |

### Tabela de Pagamentos (Payments)
| Coluna                    | Tipo               | Descrição                   |
|---------------------------|:------------------:|-----------------------------|
| order_id                  | INT                | Identificador vinculado ao pedido. |
| id                        | INT                | Identificador único do pagamento. |
| method                    | STRING             | Método de pagamento (dinheiro, cartão de crédito, etc.). |
| amount                    | DECIMAL(15,2)      | Valor total pago.           |
| tranch_value              | DECIMAL(15,2)      | Valor de cada parcela (se aplicável). |
| tranch_payment_date       | DATE               | Data do pagamento da parcela. |
| tranch_installment_number | SMALLINT           | Número da parcela.          |

---

## Requisitos

- Python 3.13 ou superior
- PySpark
- JARs de runtime do Iceberg
- Ambiente Hadoop
- UDF em Scala (função de descompactação)

---

## Licença

Este projeto está licenciado sob a licença MIT. Veja [LICENSE](./LICENSE) para mais detalhes.

---

## Contribuição

Contribuições são bem-vindas! Por favor, faça um fork do repositório e envie um pull request.

---

## Contato

Para dúvidas ou suporte, entre em contato pelo e-mail [leonardovsr.dev@gmail.com].

---

# Python ETL Pipeline: Transforming compressed JSON into Iceberg tables

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