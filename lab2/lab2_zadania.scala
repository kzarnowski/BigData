// Databricks notebook source
// MAGIC %md
// MAGIC Zadanie 1

// COMMAND ----------

// MAGIC %run "/Mini-Kurs-ETL-Spark/4. Scala/3. Pobierz Dane"

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/Files/"))

// COMMAND ----------

import org.apache.spark.sql.types.{VarcharType,ArrayType,ByteType, StringType,StructType,StructField}

val schema = StructType(Array(
  StructField("imdb_title_id", StringType, true),
  StructField("ordering", ByteType, true),
  StructField("imdb_name_id", StringType, true),
  StructField("category", StringType, true),
  StructField("job", StringType, true),
  StructField("characters", StringType, true)
))

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/actors.csv"
val actorsDf = spark.read.format("csv")
            .option("header","true")
            .schema(schema)
            .load(filePath)

// COMMAND ----------

actorsDf.printSchema()

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/actors.csv"
val actorsDf_defaultSchema = spark.read.format("csv")
            .option("header","true")
            .option("inferSchema", "true")
            .load(filePath)

// COMMAND ----------

actorsDf_defaultSchema.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 2

// COMMAND ----------

display(actorsDf)

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 4

// COMMAND ----------

Seq(
  "{'imdb_title_id':'tt0000009', 'ordering':'1', 'imdb_name_id':'nm0063086', 'category':'actress', 'job':null, 'characters':'[Miss Geraldine Holbrook (Miss Jerry)]'}",
  "{'imdb_title_id':'tt0000009', 'ordering':'2', 'imdb_name_id':'nm0183823', 'category':'actor', 'job':null, 'characters':'[Mr. Hamilton]'}",
  "{'imdb_title_id':11, 'ordering'::::::'3', 99999, , , , )}" // bad record
).toDF().write.mode("overwrite").text("/FileStore/tables/lab2/zad4.json")

// COMMAND ----------

var df_1 = spark.read.format("json")
  .schema(schema)
  .option("mode", "permissive")
  .load("/FileStore/tables/lab2/zad4.json")

display(df_1)

// COMMAND ----------

var df_2 = spark.read.format("json")
  .schema(schema)
  .option("mode", "DROPMALFORMED")
  .load("/FileStore/tables/lab2/zad4.json")

display(df_2)

// COMMAND ----------

var df_3 = spark.read.format("json")
  .schema(schema)
  .option("mode", "FAILFAST")
  .load("/FileStore/tables/lab2/zad4.json")

display(df_3)

// COMMAND ----------

var df_4 = spark.read.format("json")
  .schema(schema)
  .option("badRecordsPath", "/FileStore/tables/lab2/bad_records")
  .load("/FileStore/tables/lab2/zad4.json")

display(df_4)

// COMMAND ----------

// MAGIC %md
// MAGIC Opcja permissive ustawia błędne pola na null, DROPMALFORMED usuwa całe wiersze zawierające błędne dane, FAILFAST wypisuje komunikat o błędzie po napotkaniu jakichkolwiek błędnych danych i przerywa całą procedurę, opcja z badRecordsPath zapisuje błędne rekordy do pliku podanego jako argument.

// COMMAND ----------

df_1.write.format("parquet").mode("overwrite").save("/FileStore/tables/df_1.parquet")
df_1.write.format("json").mode("overwrite").save("/FileStore/tables/df_1.json")

val df_1a = spark.read.format("parquet").load("/FileStore/tables/df_1.parquet")
val df_1b = spark.read.format("json").load("/FileStore/tables/df_1.json")

display(df_1a)
display(df_1b)

// COMMAND ----------

// MAGIC %md
// MAGIC Sam plik .parqueet został skompresowany więc nie da się z niego nic odczytać bezpośrednio, ale po wczytaniu do data frame dane są poprawne.
