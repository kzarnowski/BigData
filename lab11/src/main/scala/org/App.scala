package org
import org.transformations.Filter
import org.apache.spark.sql.{ SparkSession}
import org.apache.spark.sql.DataFrame
import org.case_class.Diamonds
import org.data.{DataReader, DataWriter}


object App{

  def main(args: Array[String]) : Unit =
  {
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Maven_first_app")
      .getOrCreate();

    import spark.implicits._
    val reader= new DataReader();

    val diamonds_df:DataFrame=reader.read_csv("C:\\Users\\krzys\\Downloads\\lab11\\diamods.csv", spark.sqlContext, header = true );
    diamonds_df.show(10);

    val diamonds_dataset = diamonds_df.as[Diamonds];
    val filter = new Filter();

    val filtered = diamonds_dataset.filter(row => filter.expensive(row, 1000))

    val writer=new data.DataWriter();
    writer.write(filtered.toDF(),"C:\\Users\\krzys\\Downloads\\lab11\\summary.csv");


  }




}
