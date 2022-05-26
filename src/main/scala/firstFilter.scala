//import org.apache.hive._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object firstFilter {

  case class Customer(OrderId:Int,CusId:Int,CustomerName:String,ProductName:String,Category:String,ProductPrice:Int,Qty:Double,
                      PayType:String,Valid:String,DatePurchased:String,Country:String,Website:String)

  def parseLine(line: String): (String, String) = {
    // Split by commas
    val fields = line.split(",")
    // Extract the age and numFriends fields, and convert to integers
    val products = fields(3)
    val date = fields(9)
    // Create a tuple that is our result.
    (products, date)
  }
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
    val Customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("Data/Project1.csv")
      .as[Customer]


//    Customers.printSchema()
//
    Customers.createOrReplaceTempView("people")
    //first Query
//    val TopCategoryPerCountry =  spark.sql("SELECT count(ProductName),max(ProductName),Country FROM people as purchases GROUP BY Country")
    //second Query
//    val PopularProducts = spark.sql("SELECT ProductName,DatePurchased,Country FROM people WHERE DatePurchased LIKE '2022%' SORT By DatePurchased ASC")
//    val results = PopularProducts.groupBy("ProductName","Country").count().collect()

    //3rd Query
//    val firstHiveQuery = spark.sql("SELECT ProductPrice,Country FROM people Order By ProductPrice DESC")
//    firstHiveQuery.groupBy("Country").sum("ProductPrice").sort($"sum(ProductPrice)".desc).show(1)

//    results.foreach(println)
//    results1.foreach(println)
    spark.stop()
  }
}
