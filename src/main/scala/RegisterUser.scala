import scala.collection.mutable
import scala.io.StdIn.readLine
import scala.util.Random

class RegisterUser() {


  val nuUsers = new mutable.HashMap[Int,(String,String)]()
  def generateId(): Int = {
    val generate = 0 to 100
    val newIDs = generate(Random.nextInt(generate.length))
    newIDs
  }
    val UserName = readLine("Enter a Username :  ")
    val Password = readLine("Enter a Password :  ")

};
