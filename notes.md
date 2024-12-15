# Processamento de JSON Altamente Comprimidos em Larga Escala na AWS 🚀

Imagine lidar com um banco de dados OLTP em produção sem indexação, com baixa performance e bilhões de registros nas tabelas principais do negócio. Parece o pesadelo de qualquer engenheiro de dados, certo? Recentemente, enfrentei exatamente esse desafio em um dos projetos em que atuei como Engenheiro de Dados.

## 🎯 O desafio:  
Extrair informações desse banco de origem e disponibilizá-las para os usuários dentro da AWS, enfrentando a lentidão e o tamanho massivo das tabelas.

## 💡 O insight:  
Durante nossa investigação, descobrimos uma coluna que armazenava as mesmas informações das tabelas com bilhões de registros, mas em formato JSON comprimido em GZIP. Apesar de otimizada para armazenamento, essa coluna continha registros com mais de **800 mil caracteres cada**!

## 🔧 A solução estratégica:  
Optamos por ingerir apenas essa coluna comprimida para o S3, processá-la e descomprimí-la usando **AWS Glue** com **PySpark**. Os dados foram transformados em múltiplas tabelas organizadas no formato **Iceberg**, otimizando o consumo via **Athena**. Além disso, criamos visões sumarizadas no **Data Warehouse**, tornando os dados mais acessíveis e prontos para análise.

## 🚀 Explorando diferentes abordagens:  
Testamos descompressão e transformação com:
- **Pandas + UDFs do PySpark**: eficiente, mas com sobrecarga de transformação entre Python e JVM.  
- **Scala**: eliminou a transformação Python → JVM, entregando melhor performance.

## 💾 Simulação e reprodutibilidade:  
Criamos um repositório no GitHub para simular o comportamento do banco, gerando arquivos Parquet que replicam a estrutura original. A partir disso, processamos os dados e os disponibilizamos em tabelas **Iceberg** com o catálogo no Hadoop, compartilhando práticas de organização e performance.

Essa experiência reforçou algo que sempre acredito: mesmo em cenários adversos, com análise detalhada e escolhas estratégicas, é possível encontrar soluções robustas para problemas complexos de dados.

Se você já enfrentou desafios similares, compartilhe nos comentários! Vamos trocar ideias e experiências! 🚀💬  

----

# Processing Highly Compressed JSON at Scale on AWS 🚀

Imagine dealing with a production OLTP database with no indexing, poor performance, and billions of records in the main business tables. Sounds like a nightmare for any data engineer, right? Recently, I faced exactly this challenge in one of the projects where I worked as a Data Engineer.

## 🎯 The Challenge:  
Extract information from this source database and make it available to users within AWS, despite the sluggish performance and massive size of the tables.

## 💡 The Insight:  
During our investigation, we discovered a column that stored the same information as the tables with billions of records but in JSON format compressed with **GZIP**. While optimized for storage, this column contained records with more than **800,000 characters each**!

## 🔧 The Strategic Solution:  
We decided to ingest only this compressed column into **S3**, process it, and decompress it using **AWS Glue** with **PySpark**. The data was transformed into multiple tables organized in the **Iceberg** format, optimizing consumption through **Athena**. Additionally, we created summarized views in the **Data Warehouse**, making the data more accessible and analysis-ready.

## 🚀 Exploring Different Approaches:  
We tested decompression and transformation using:
- **Pandas + PySpark UDFs**: efficient but with the overhead of transforming between Python and the JVM.  
- **Scala**: eliminated the Python → JVM transformation, resulting in better performance.

## 💾 Simulation and Reproducibility:  
We created a GitHub repository to simulate the database behavior by generating Parquet files that replicate the original structure. From there, we processed the data and made it available in **Iceberg** tables using a Hadoop catalog, sharing best practices for organization and performance.

This experience reinforced something I always believe: even in adverse scenarios, with detailed analysis and strategic decisions, it is possible to find robust solutions for complex data processing challenges.

If you’ve faced similar challenges, share your experiences in the comments! Let’s exchange ideas and insights! 🚀💬
