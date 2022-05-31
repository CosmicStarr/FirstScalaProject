

object Menus {

  def WelcomeText() : Unit ={
    println(s"Welcome To BigCosmicData\nWe do things at a Universal Level!")
    HomeMenu()
  }
  def HomeMenu() : Unit = {
    println("[0]Exit")
    println("[1]Profile")
    println("[2]Available Queries")
  }
  def Queries() : Unit = {
    println("[0]Exit")
    println("[1]Query the world with Spark Sql")
    println("[2]Query the world with Hive")
  }
  def ActualQueries() : Unit ={
    println("[0]Exit")
    println("[1]What is the top selling category of items? Per Country?")
    println("[2]How does the popularity of products change throughout the year? Per Country?")
    println("[3]Which locations see the highest traffic of sales?")
    println("[4]What times have the highest traffic of sales? Per Country?")
  }
  def ChooseAQuery() : Unit = {
    println("[0]Exit")
    println("[1]Total amount of products each country accumulated")
    println("[2]Query the world with Hive Query")
  }
}
