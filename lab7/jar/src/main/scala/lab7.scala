import org.apache.spark.sql.SparkSession
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.functions.col

import java.net.URL

object lab7 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val url_file = new URL("https://raw.githubusercontent.com/tidyverse/ggplot2/main/data-raw/diamonds.csv")
    val csv_file = IOUtils.toString(url_file,"UTF-8")
      .lines
      .toList
      .toDS()

    val df = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(csv_file)

    val df_2 = df
      .withColumn("Price in PLN",col("price")*4.30)
      .withColumn("Volume", col("x") * col("y") * col("z"))
  }
}
