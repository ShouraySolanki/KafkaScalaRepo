package com.kafka.example

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.io.ByteArrayOutputStream
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.time.Duration
import java.util
import java.util.UUID.randomUUID
class Processor{


  val producerExample = new ProducerSend

  val consumerExample = new CreateConsumer
  val kafkaConsumer: KafkaConsumer[String,String] = consumerExample.consume

  // connect to the database named "mysql" on the localhost


  // there's probably a better way to do this


  def process(): Unit = {

    kafkaConsumer.subscribe(util.Arrays.asList("jsontest"))
    kafkaConsumer.poll(0)
    kafkaConsumer.seekToBeginning(kafkaConsumer.assignment())

    while (true){
      val records = kafkaConsumer.poll(Duration.ZERO)

      records.forEach{ rec =>
        try {

          val objectMapper = new ObjectMapper() with ScalaObjectMapper
          objectMapper.registerModule(DefaultScalaModule)
          val message = rec.value()

          val sum: Sum = objectMapper.readValue[Sum](message)
          println(sum.getA())
          println(sum.getB())

          val out = new ByteArrayOutputStream()


          sum.setSum(sum.getA() + sum.getB())
          objectMapper.writeValue(out, sum)

          val json = out.toString

          producerExample.produce("jsontest1", json)
          // make the connection
          val driver = "com.mysql.jdbc.Driver"
          Class.forName(driver)

          var connection:Connection = null
          connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/ScalaDb", "root", "Laksul@776434426")

          // create the statement, and run the select query


          val insertSql = """
                            |insert into sum (id,a,b,sum)
                            |values (?,?,?,?)
                            |""".stripMargin

          val preparedStmt: PreparedStatement = connection.prepareStatement(insertSql)

          preparedStmt.setString(1, randomUUID().toString)
          preparedStmt.setInt (2, sum.getA())
          preparedStmt.setInt  (3, sum.getB())
          preparedStmt.setInt  (4, sum.getSum())
          preparedStmt.execute

          preparedStmt.close()
        }
        catch{
          case e:JsonProcessingException => throw e
        }


      }


    }


  }
}