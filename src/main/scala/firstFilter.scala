//import org.apache.hive._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object firstFilter {

  case class Customer(OrderId:Int,CusId:Int,CustomerName:String,ProductName:String,Category:String,ProductPrice:Int,Qty:Double,
                      PayType:String,Valid:String,DatePurchased:String,Country:String,Website:String)
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

    schemaPeople.printSchema()

    schemaPeople.createOrReplaceTempView("people")
    val filteredPeople =  spark.sql("SELECT count(ProductName),max(ProductName),Country FROM people as purchases GROUP BY Country")

//    spark.sql("SELECT product, category,FROM (SELECT product, category, revenue, dense_rank() OVER (PARTITION BY category ORDER BY revenue DESC) as rank FROM productRevenue)")



    val results = filteredPeople.collect()

    results.foreach(println)

    spark.stop()
  }
}
