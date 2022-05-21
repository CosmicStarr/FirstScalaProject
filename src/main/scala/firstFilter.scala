//import org.apache.hive._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
object firstFilter {

  case class Customer(Id:Int, CustomerName:String, ProductID:Int, ProductName:String, ProductPrice:Int, Qty:Int, PayType:String, Valid:String, DatePurchased:DateTime, Country:String)
  def main(args: Array[String]): Unit ={

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use SparkSession interface
    val spark = SparkSession
      .builder
      .appName("firstFilter")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    // Load each line of the source data into an Dataset
    import spark.implicits._
    val schemaPeople = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("Data/Project1.csv")
      .as[Customer]

//    schemaPeople.printSchema()

    schemaPeople.createOrReplaceTempView("people")
//    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    val results = schemaPeople.collect()

    results.foreach(println)

    spark.stop()
  }
}
