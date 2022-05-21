import java.time.LocalDate
import scala.util.Random

object generator {
  def main(args: Array[String]): Unit ={
    var generate = 0
    while (generate < 5){
      val orderId = 1 to 10000
      val productId = 10000 to 30000
      val productPrice = 100 to 1200
      val qty = 1 to 2
      val start = LocalDate.of(2019, 1, 20)
      val end   = LocalDate.of(2022, 2, 12)
      val valid = List("Invalid","Valid")
      val productNames = List("Samsung Tablet","S21 cell phone","Toaster","Dell Computer","Sony Speakers","Sony Phone","IPhone 13","Beats Headphones",
      "Sony Headphones","Sony Home theater","Hp Compaq Monitor","Xbox Series X","Sony PlayStation 5","Xbox Series S","Tozo Wireless Earbubs","Airpods",
      "Sony External Hard drive","Sony Router","Sony FlatScreen TV","Sony PlayStation 4","S22 cell phone","IPhone 14","Vizio TV","PlayStation Controller",
        "Xbox Controller","Optiplex 990 Desktop","Samsung Refrigerator")
      val paymentType = List("Card", "Check", "Internet Banking")
      val customerNames = List("Normand Michel", "Danny hue", "Jerry Jean",
        "Ken Masters", "Micheal Gus", "Michelle Otter", "Morgan Freeman", "Sherrell Lattes", "Ben Hues", "Chris Gene")
      val countries = List("Afghanistan","Albania","Algeria","Andorra","Angola","Antigua","Barbuda","Argentina","Armenia","Austria",
        "Azerbaijan","Bahrain","nBangladesh","nBarbados","Belarus","Belgium","Belize","Benin","Bhutan","Bolivia","Bosnia","Herzegovina","Botswana",
        "Brazil","Brunei","Bulgaria","Burkina Faso","Burundi","Verde","Cambodia","Cameroon","Canada","Central African Republic","Channel Islands",
          "Chile","China","Colombia","Comoros","Congo","Congo","Germany","Hong Kong","Hungary","India","Indonesia","Italy","Japan","North Korea",
        "Philippines","Portugal","Romania","Russia","Singapore","South Africa","Togo","Trinidad","Tobago","Tunisia","Turkey","Ukraine","United Arab Emirates",
        "United Kingdom","United States")
      //the tuple fields i can actually create a match method
      val newIDs = orderId(Random.nextInt(orderId.length))
      val newNames = customerNames(Random.nextInt(customerNames.length))
      val newProductIDs = productId(Random.nextInt(productId.length))
      val newProductNames = productNames(Random.nextInt(productNames.length))
      val newPrice = productPrice(Random.nextInt(productPrice.length))
      val newQty = qty(Random.nextInt(qty.length))
      val newPayType = paymentType(Random.nextInt(paymentType.length))
      val validPay = valid(Random.nextInt(valid.length))
      val newDate = LocalDate.ofEpochDay(Random.between(start.toEpochDay, end.toEpochDay))
      val newCountries = countries(Random.nextInt(countries.length))
      //The actually Tuple that will create the csv data
      println("Id","CustomerName","ProductID","ProductName","ProductPrice","Qty","PayType","Valid","DatePurchased","Country")
      val dataSet = Tuple1(newIDs, newNames, newProductIDs, newProductNames,newPrice,
        newQty, newPayType, validPay, newDate,newCountries)
      val info = dataSet.productIterator.distinct
       info.foreach(i => println(i))

//      var numInfo1 = dataSet.productElement(1)
//      var numInfo2 = dataSet.productElement(2)
//      var info2 = info.foreach(i => println(i))
      generate += 1
    }
//    val userInfo: Seq[(Int, String, Int,String,Int,Int,String,String,DateTime,String)] = Seq((1, "ABC", "Morgan"), (2, "BEO", "Jane"),
//      (3, "IEY", "Billy"))
//
//    // create DataFrame for the input with my colum names. AKA, put data into a table with labels.
//    val userInfo_df: DataFrame = spark.createDataFrame(userInfo).toDF("User ID", "Letter Code",
//      "First Name")
//
//    // save the data as a .csv file. It's now also an Excel sheet in my project directly in C:\ Drive.
//    // After this ran once, I got an error: "already exists" after that, so I commented it out.
//    userInfo_df.write.format("csv").save("userInfo")

  }
}
