package com.kafka.example

import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.sql.{Connection, DriverManager, PreparedStatement}

object ConnectDb {

  val consumerExample = new CreateConsumer
  val kafkaConsumer: KafkaConsumer[String,String] = consumerExample.consume
  def sendData(a:Int, b:Int, sum:Int, avg:Int) {
    // connect to the database named "mysql" on the localhost
    val driver = "com.mysql.jdbc.Driver"

    // there's probably a better way to do this
    var connection: Connection = null
    val r = scala.util.Random

        try {

          // make the connection
          Class.forName(driver)
          connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/ScalaDb", "root", "Laksul@776434426")

          // create the statement, and run the select query


          val insertSql =
            """
              |insert ignore into average (id,a,b,sum,average)
              |values (?,?,?,?,?)
              |""".stripMargin

          val preparedStmt: PreparedStatement = connection.prepareStatement(insertSql)

          preparedStmt.setInt(1, r.nextInt(1000))
          preparedStmt.setInt(2, a)
          preparedStmt.setInt(3, b)
          preparedStmt.setInt(4, sum)
          preparedStmt.setInt(5,avg )

          preparedStmt.execute

          preparedStmt.close()

        }
        catch{
          case e:JsonProcessingException => throw e
        }


      }



  }


