import java.time.LocalDate
import scala.util.Random

object generator {
  def main(args: Array[String]): Unit ={
    println("OrderId","CusId","CustomerName","ProductID","ProductName","Category","ProductPrice","Qty","PayType","Valid","DatePurchased","Country","Website")
    var generate = 0
    while (generate < 5){
      val orderId = 1 to 10000
//      val productId = 10000 to 30000
//      val productPrice = 299 to 1299
      val qty = 1 to 2
      val start = LocalDate.of(2019, 1, 20)
      val end   = LocalDate.of(2022, 6, 24)
      val webSites = List("www.Amazon.com","www.Wal-Mart.com","www.Samsung.com")
      val valid = List("Invalid","Valid")
      val productNames = List((1,"Samsung Tablet","Tablets",299),(2,"S21 cell phone","Phones",1299),(3,"Sony Desktop","Computers",599),
        (4,"Dell Computer","Computers",599),(5,"Sony Speakers","Audio",299),(6,"Sony Phone","Phones",1199),(7,"IPhone 13","Phones",1299),(8,"Beats Headphones","Headphones",399),
      (9,"Sony Headphones","Headphones",399),(10,"Sony Home theater","Home theater",499),(11,"Hp Compaq Monitor","Computers",499),(12,"Xbox Series X","Video games",599),
        (13,"Sony PlayStation 5","Video games",499),(14,"Xbox Series S","Video games",499),(15,"Tozo Wireless Earbubs","Headphones",29),(16,"Airpods","Headphones",199),
      (17,"Sony External Hard drive","Computers",99),(18,"Sony Router","Computers",99),(19,"Sony FlatScreen TV","TV's",1199),(20,"Sony PlayStation 4","Video games",299),
        (21,"S22 cell phone","Phones",1299),(22,"IPhone 14","Phones",1299),(23,"Vizio TV","TV's",199),(24,"PlayStation Controller","Video games",49),
        (25,"Xbox Controller","Video games",49),(26,"Optiplex 990 Desktop","Computers",499),(27,"Samsung Refrigerator","Appliances",599),(28,"Whirlpool dishwasher","Appliances",799),
        (29,"Samsung Washer","Appliances",799),(30,"Samsung Dryer","Appliances",799),(31,"Maytag Washer","Appliances",899),(32,"Samsung Stove","Appliances",699),
        (33,"Hamilton Beach Microwave","Appliances",79))
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
//      val newProductIDs = productId(Random.nextInt(productId.length))
      val newProductNames = productNames(Random.nextInt(productNames.length))
//      val newPrice = productPrice(Random.nextInt(productPrice.length))
      val newQty = qty(Random.nextInt(qty.length))
      val newPayType = paymentType(Random.nextInt(paymentType.length))
      val validPay = valid(Random.nextInt(valid.length))
      val newWebSite = webSites(Random.nextInt((webSites.length)))
      val newDate = LocalDate.ofEpochDay(Random.between(start.toEpochDay, end.toEpochDay))
      val newCountries = countries(Random.nextInt(countries.length))
      //The actually Tuple that will create the csv data
      val dataSet = Tuple1(newIDs,newNames, newProductNames,
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
