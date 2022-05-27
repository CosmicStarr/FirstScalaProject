//import org.apache.hive._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


import scala.io.StdIn.readInt
object firstFilter {

  case class Customer(OrderId:Int,CusId:Int,CustomerName:String,ProductName:String,CatID:Int,Category:String,ProductPrice:Int,Qty:Int,
                      PayType:String,Valid:String,DatePurchased:String,CountryID:Int,Country:String,Website:String)

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

      Menus.WelcomeText()
      println("Choose:")
      var options = readInt()
      println($"You Chose : ${options}")
      while (options != 0){
      if (options == 2){
        println("Available Options")
        Menus.Queries()
        println("Choose:")
        var options1 = readInt()
        println(s"You Chose : ${options1}")
        if(options1 == 1){
          while (options1 != 0){
            println("Initializing...")
            Customers.createOrReplaceTempView("people")
            println("Get Ready to Query the world!")
            //first Query I have to fix it. i need to give the categories IDs
            val TopCategoryPerCountry = Customers.select("Category","Country","ProductPrice")
            TopCategoryPerCountry.groupBy("Category","Country").sum("ProductPrice").sort("Country").toDF().show()
            Menus.Queries()
            println(s"What else do you want to do? Choose an Option =>")
            options1 = readInt()
            if(options1 == 2){
              options = 2
              options1 = 0
            }
            else if(options1 == 0){
              Menus.HomeMenu()
              println("Choose:")
              options = readInt()
              println($"You Chose : ${options}")
            }
            else {
              println("Invalid")
            }
          }
        }
      }
    }
    println("Thank you for visiting BigCosmicData")




    //second Query
//    val PopularProducts1 = spark.sql("SELECT * FROM people WHERE DatePurchased LIKE '2022%' SORT By DatePurchased ASC")
//    PopularProducts.groupBy("ProductName","Country").count().show(50)
//    val PopularProducts2 = spark.sql("SELECT * FROM people WHERE DatePurchased LIKE '2021%' SORT By DatePurchased ASC")
//    PopularProducts2.groupBy("ProductName","Qty","Country").count().sort("Country").show(50)
//      val PopularProducts3 = spark.sql("SELECT ProductName,Country FROM people WHERE DatePurchased LIKE '2019%' SORT By DatePurchased ASC")
//      PopularProducts3.groupBy("ProductName","Country").count().show(50)

    //3rd Query
//    select country, max(sqty)
//    select country, max(sqty)
//    from Project1
//      group by country;



    //    val firstHiveQuery = spark.sql("SELECT * FROM people")
//    firstHiveQuery.groupBy("Country").sum("ProductPrice").sort($"sum(ProductPrice)".desc).show()
    //4th Query
//    val firstHiveQuery2 = spark.sql("SELECT * FROM people")
//    firstHiveQuery2.groupBy("DatePurchased","Country").sum("ProductPrice").sort($"sum(ProductPrice)".desc).show()

//    TopCategoryPerCountry.foreach(println)
//    results1.foreach(println)

    spark.stop()
  }
}
