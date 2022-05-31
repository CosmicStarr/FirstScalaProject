import java.time.LocalDate
import scala.util.Random

object generator {
  def main(args: Array[String]): Unit ={
//    println("OrderId","CusId","CustomerName","ProductID","ProductName","CatID","Category","ProductPrice","Qty","PayType","Valid","DatePurchased","CountryID","Country","Website")
    var generate = 0
    while (generate < 5000){
      val orderId = 1 to 10000
//      val productId = 10000 to 30000
//      val productPrice = 299 to 1299
      val qty = 1 to 2
      val start = LocalDate.of(2019, 1, 20)
      val end   = LocalDate.of(2022, 6, 24)
      val webSites = List("www.Amazon.com","www.Wal-Mart.com","www.Samsung.com")
      val valid = List("Invalid","Valid")
      val productNames = List((1,"Samsung Tablet",(1,"Tablets"),299),(2,"S21 cell phone",(2,"Phones"),1299),(3,"Sony Desktop",(3,"Computers"),599),
        (4,"Dell Computer",(3,"Computers"),599),(5,"Sony Speakers",(4,"Audio"),299),(6,"Sony Phone",(2,"Phones"),1199),(7,"IPhone 13",(2,"Phones"),1299),(8,"Beats Headphones",(5,"Headphones"),399),
      (9,"Sony Headphones",(5,"Headphones"),399),(10,"Sony Home theater",(6,"Home theater"),499),(11,"Hp Compaq Monitor",(3,"Computers"),499),(12,"Xbox Series X",(7,"Video games"),599),
        (13,"Sony PlayStation 5",(7,"Video games"),499),(14,"Xbox Series S",(7,"Video games"),499),(15,"Tozo Wireless Earbubs",(5,"Headphones"),29),(16,"Airpods",(5,"Headphones"),199),
      (17,"Sony External Hard drive",(3,"Computers"),99),(18,"Sony Router",(3,"Computers"),99),(19,"Sony FlatScreen TV",(9,"TV's"),1199),(20,"Sony PlayStation 4",(7,"Video games"),299),
        (21,"S22 cell phone",(2,"Phones"),1299),(22,"IPhone 14",(2,"Phones"),1299),(23,"Vizio TV",(9,"TV's"),199),(24,"PlayStation Controller",(7,"Video games"),49),
        (25,"Xbox Controller",(7,"Video games"),49),(26,"Optiplex 990 Desktop",(3,"Computers"),499),(27,"Samsung Refrigerator",(8,"Appliances"),599),(28,"Whirlpool dishwasher",(8,"Appliances"),799),
        (29,"Samsung Washer",(8,"Appliances"),799),(30,"Samsung Dryer",(8,"Appliances"),799),(31,"Maytag Washer",(8,"Appliances"),899),(32,"Samsung Stove",(8,"Appliances"),699),
        (33,"Hamilton Beach Microwave",(8,"Appliances"),79))
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
      val countries = List((1,"Afghanistan"),(2,"Albania"),(3,"Algeria"),(4,"Andorra"),(5,"Angola"),(5,"Antigua"),(6,"Barbuda"),(7,"Argentina"),(8,"Armenia"),(9,"Austria"),
        (10,"Azerbaijan"),(11,"Bahrain"),(12,"Bangladesh"),(13,"Barbados"),(14,"Belarus"),(15,"Belgium"),(16,"Belize"),(17,"Benin"),(18,"Bhutan"),(19,"Bolivia"),(20,"Bosnia"),
        (22,"Herzegovina"),(23,"Botswana"),(24,"Brazil"),(25,"Brunei"),(26,"Bulgaria"),(27,"Burkina Faso"),(28,"Burundi"),(29,"Verde"),(30,"Cambodia"),(31,"Cameroon"),(32,"Canada"),
        (33,"Central African Republic"),(34,"Channel Islands"),(35,"Chile"),(36,"China"),(37,"Colombia"),(38,"Comoros"),(39,"Congo"),(40,"Congo"),(41,"Germany"),(42,"Hong Kong"),(43,"Hungary"),
        (44,"India"),(45,"Indonesia"),(46,"Italy"),(47,"Japan"),(48,"North Korea"),
        (49,"Philippines"),(50,"Portugal"),(51,"Romania"),(52,"Russia"),(53,"Singapore"),(54,"South Africa"),(55,"Togo"),(56,"Trinidad"),(57,"Tobago"),(58,"Tunisia"),(59,"Turkey"),
        (60,"Ukraine"),(61,"United Arab Emirates"),(62,"United Kingdom"),(63,"United States"))
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
