import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

import scala.io.StdIn.{readInt, readLine}

object firstFilter {

  case class Customer(OrderId: Int, CusId: Int, CustomerName: String, ProductName: String, CatID: Int, Category: String, ProductPrice: Int, Qty: Int,
                      PayType: String, Valid: String, DatePurchased: String, CountryID: Int, Country: String, Website: String)

  def parseLine(line: String): (String, String) = {
    // Split by commas
    val fields = line.split(",")
    // Extract the age and numFriends fields, and convert to integers
    val products = fields(3)
    val date = fields(9)
    // Create a tuple that is our result.
    (products, date)
  }

  def main(args: Array[String]): Unit = {
    //    System.setProperty("hadoop.home.dir","C:/hadoop/bin")

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


    //Hive Info
    //        spark.sql("CREATE TABLE IF NOT EXISTS src (OrderId INT, CusId INT,CustomerName STRING,ProductName STRING," +
    //          "CatID INT,Category STRING,ProductPrice INT,Qty INT,PayType STRING,Valid STRING,DatePurchased STRING,CountryID INT," +
    //          "Country STRING,Website STRING) USING hive")
    //        spark.sql("LOAD DATA LOCAL INPATH 'Data/Project1.csv' INTO TABLE src")

    val reg = new RegisterUser()
    val UserName = reg.UserName
    val Password = reg.Password
    val UserId = reg.generateId()
    reg.nuUsers.put(UserId, (UserName, Password))
    println(s"Wonderful $UserName This is your ID : $UserId")
    println("Try to login!")

    val loginUsr = readLine("Enter your Username :  ")
    val passUsr = readLine("Enter your Password :  ")
    val nuUser = (loginUsr, passUsr)
    val getUsers = reg.nuUsers.get(UserId)
    if (getUsers.contains(nuUser)) {
      Menus.WelcomeText()
      println("Choose:")
      var options = readInt()
      println(s"You Chose : $options")
      while (options != 0) {
        if (options == 2) {
          println("Available Options")
          Menus.Queries()
          println("Choose:")
          var options1 = readInt()
          println(s"You Chose : $options1")
          if (options1 == 1) {
            while (options1 != 0) {
              Menus.ActualQueries()
              var option2 = readInt()
              while (option2 != 0) {
                if (option2 == 1) {
                  println("Initializing...")
                  Customers.createOrReplaceTempView("people")
                  println("Get Ready to Query the world!")
                  //1st Query. I have to fix it. I have to find a way to get the max cat sales per country
                  spark.sql("Select sum(ProductPrice) as Price,ProductName,Category,Country From people Group By ProductName,Category,Country Having Price >= 15000 Order By ProductName,Country").show(1000)
                  //val TopCategoryPerCountry = Customers.select("ProductName","Category","Country","ProductPrice")
                  //TopCategoryPerCountry.groupBy("ProductName","Category","Country").sum("ProductPrice").sort("ProductName","Country").toDF().show()
                  Menus.ActualQueries()
                  println(s"What else do you want to do? Choose an Option =>")
                  option2 = readInt()
                }
                else if (option2 == 2) {
                  println("Initializing...")
                  println("Ready!")
                  println("The Year 2022")
                  val PopularProducts1 = spark.sql("SELECT * FROM people WHERE DatePurchased LIKE '2022%' SORT By DatePurchased ASC")
                  PopularProducts1.groupBy("ProductName", "Country").count().show(50)
                  println("The Year 2021")
                  val PopularProducts2 = spark.sql("SELECT * FROM people WHERE DatePurchased LIKE '2021%' SORT By DatePurchased ASC")
                  PopularProducts2.groupBy("ProductName", "Qty", "Country").count().sort("Country").show(50)
                  println("The Year 2019")
                  val PopularProducts3 = spark.sql("SELECT ProductName,Country FROM people WHERE DatePurchased LIKE '2019%' SORT By DatePurchased ASC")
                  PopularProducts3.groupBy("ProductName", "Country").count().show(50)
                  Menus.ActualQueries()
                  println(s"What else do you want to do? Choose an Option =>")
                  option2 = readInt()
                }
                else if (option2 == 3) {
                  val firstHiveQuery = spark.sql("SELECT * FROM people")
                  firstHiveQuery.groupBy("CusId").sum("ProductPrice").show(1000)
                  Menus.ActualQueries()
                  println(s"What else do you want to do? Choose an Option =>")
                  option2 = readInt()
                }
                else if (option2 == 4) {
                  val firstHiveQuery2 = spark.sql("SELECT * FROM people")
                  firstHiveQuery2.groupBy("DatePurchased", "Country").sum("ProductPrice").sort($"sum(ProductPrice)".desc).show()
                  Menus.ActualQueries()
                  println(s"What else do you want to do? Choose an Option =>")
                  option2 = readInt()
                }
                else {
                  println("Invalid Selection!")
                  Menus.ActualQueries()
                  println(s"What else do you want to do? Choose an Option =>")
                  option2 = readInt()
                }
              }
              Menus.Queries()
              println(s"What else do you want to do? Choose an Option =>")
              options1 = readInt()
              if (options1 == 2) {
                options = 2
                options1 = 0
              }
              else if (options1 == 0) {
                Menus.HomeMenu()
                println("Choose:")
                options = readInt()
                println(s"You Chose : $options")
              }
              else {
                println("Invalid")
              }
            }
          }
        }
      }
    } else {
      println("Try Again!")
    }
    println("Thank you for visiting BigCosmicData")




    //    val plot = Vegas("maxSalesPerCountry").withData(Seq(
    //      Map("Country" -> "Brazil", "Qty" -> "200"),
    //      Map("Country" -> "Austria", "Qty" -> "4000"),
    //      Map("Country" -> "North Korea", "Qty" -> "100"))).
    //      encodeX("Country", Nom).
    //      encodeY("Qty", Quant).
    //      mark(Bar)
    //    plot.show

    //    Customers.createOrReplaceTempView("people")
    //   spark.sql("Select count(OrderId) as orders,sum(ProductPrice) as Price,ProductName,Category,Country " +
    //     "From people Group By ProductName,Category,Country Having Price > 10000 Order By Category,Country").show(1000)


    //2nd Query
    //    val PopularProducts1 = spark.sql("SELECT * FROM people WHERE DatePurchased LIKE '2022%' SORT By DatePurchased ASC")
    //    PopularProducts.groupBy("ProductName","Country").count().show(50)
    //    val PopularProducts2 = spark.sql("SELECT * FROM people WHERE DatePurchased LIKE '2021%' SORT By DatePurchased ASC")
    //    PopularProducts2.groupBy("ProductName","Qty","Country").count().sort("Country").show(50)
    //    val PopularProducts3 = spark.sql("SELECT ProductName,Country FROM people WHERE DatePurchased LIKE '2019%' SORT By DatePurchased ASC")
    //    PopularProducts3.groupBy("ProductName","Country").count().show(50)

    //3rd Query
    //    val firstHiveQuery = spark.sql("SELECT * FROM people")
    //    firstHiveQuery.groupBy("CusId").sum("ProductPrice").show(1000)
    //4th Query
    //    val firstHiveQuery2 = spark.sql("SELECT * FROM people")
    //    firstHiveQuery2.groupBy("DatePurchased","Country").sum("ProductPrice").sort($"sum(ProductPrice)".desc).show()

    //    TopCategoryPerCountry.foreach(println)
    //    results1.foreach(println)

    spark.stop()
  }
}
