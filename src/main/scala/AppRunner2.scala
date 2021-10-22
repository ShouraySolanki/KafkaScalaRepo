import com.kafka.example.DatabaseConsumer

object AppRunner2 {
  def main(args: Array[String]): Unit = {

//    ConnectDb.sendData()
    val databaseConsumer = new DatabaseConsumer
    println(databaseConsumer.consumeSend())
  }

}
