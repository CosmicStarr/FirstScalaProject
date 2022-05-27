

object Menus {

  def WelcomeText() : Unit ={
//    val name = readLine("What is your name? :  ")
//    val num = readInt()
    println(s"Welcome To BigCosmicData\nWe do things at a Universal Level!")
    HomeMenu()
  }
  def HomeMenu() : Unit = {
    println("[0]Exit")
    println("[1]Home")
    println("[2]Available Queries")
  }
  def Queries() : Unit = {
    println("[0]Exit")
    println("[1]Sql Query")
    println("[2]Hive Query")
  }
}
