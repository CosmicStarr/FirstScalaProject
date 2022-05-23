import java.time.LocalDate
import scala.util.Random

object generator {
  def main(args: Array[String]): Unit ={
//    println("OrderId","CusId","CustomerName","ProductID","ProductName","ProductPrice","Qty","PayType","Valid","DatePurchased","Country","Website")
    var generate = 0
    while (generate < 5000){
      val orderId = 1 to 10000
      val productId = 10000 to 30000
      val productPrice = 299 to 1299
      val qty = 1 to 2
      val start = LocalDate.of(2019, 1, 20)
      val end   = LocalDate.of(2022, 6, 24)
      val webSites = List("www.Amazon.com","www.Wal-Mart.com","www.Samsung.com")
      val valid = List("Invalid","Valid")
      val productNames = List("Samsung Tablet","S21 cell phone","Sony Desktop","Dell Computer","Sony Speakers","Sony Phone","IPhone 13","Beats Headphones",
      "Sony Headphones","Sony Home theater","Hp Compaq Monitor","Xbox Series X","Sony PlayStation 5","Xbox Series S","Tozo Wireless Earbubs","Airpods",
      "Sony External Hard drive","Sony Router","Sony FlatScreen TV","Sony PlayStation 4","S22 cell phone","IPhone 14","Vizio TV","PlayStation Controller",
        "Xbox Controller","Optiplex 990 Desktop","Samsung Refrigerator","Whirlpool dishwasher","Samsung Washer","Samsung Dryer")
      val paymentType = List("Card", "Check", "Internet Banking")
      val customerNames = List((1,"Normand Michel"), (2,"Danny hue"), (3,"Jerry Jean"),
       (4,"Ken Masters"), (5,"Micheal Gus"), (6,"Michelle Otter"), (7,"Morgan Freeman"), (8,"Sherrell Lattes"),
       (9,"Ben Hues"), (10,"Chris Gene"),(11,"Abel don"),(12,"Rose Jean"),(13,"Sophia Jean"),(14,"McKenzie Phoenix"),
       (15,"Farah Jean"),(16,"Farrah Michelle"),(17,"Christopher Michel"),(18,"Lewis Garden"),(19,"Chris garnet"),(20,"Danielle Will"),(21,"Kenneth Zah"),
        (22,"Liam Shaw"),(23,"Noah Han"),(24,"Ethan Shaw"),(25,"Melia Rage"),(26,"Kay Kiske"),(27,"Sol Badguy"),(28,"Baiken Faust"),(29,"Justice Rage"),(30,"Chipp Zanuff"),
        (31,"Baiken Hen"),(32,"Kliff Undersn"),(33,"Faust Williams"),(34,"Laura Williams"),(35,"Laurelton Shera"),(36,"Rudy Derr"),(37,"Elizabeth kind"),(38,"Honey Wells"),
        (39,"Zangieff Russ"),(40,"Carol Ritz"),(41,"Chungli Chin"),(42,"Sherry Cher"),(43,"Taylor Wind"),(44,"Karmelo Winnie"),(45,"Brian Sea"),(46,"Ronald Bensworth"),
        (47,"Cynthia Dean"),(48,"Danny Maggis"),(49,"Daniel Maggio"),(50,"Matthew Rosen"),(51,"Karen Starrs"),(52,"Tony Stark"),(53,"Louis Clark"),(54,"Luna Starr"),
        (55,"Camila Grant"),(56,"Harper Gene"),(57,"Evelyn Michels"),(58,"Mia pena"),(59,"Isabella Willford"),(60,"Amelia Crawford"),(61,"Charlotte Hanson"),(62,"Emma Streak"),
        (63,"Oliva Gems"),(64,"Nova Williow"),(65,"Aurora Grace"),(66,"Jean Hazel"),(67,"Lillian Cruz"),(68,"Addison Leah"),(69,"Marc Galdson"),(70,"Gemina Zoe"),
        (71,"Elcy Jean"),(72,"Paisley Everly"),(73,"Erick Manison"))
      val countries = List("Afghanistan","Albania","Algeria","Andorra","Angola","Antigua","Barbuda","Argentina","Armenia","Austria",
        "Azerbaijan","Bahrain","Bangladesh","Barbados","Belarus","Belgium","Belize","Benin","Bhutan","Bolivia","Bosnia","Herzegovina","Botswana",
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
      val newWebSite = webSites(Random.nextInt((webSites.length)))
      val newDate = LocalDate.ofEpochDay(Random.between(start.toEpochDay, end.toEpochDay))
      val newCountries = countries(Random.nextInt(countries.length))
      //The actually Tuple that will create the csv data
      val dataSet = Tuple1(newIDs,newNames, newProductIDs, newProductNames,newPrice,
        newQty, newPayType, validPay, newDate,newCountries,newWebSite)
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
