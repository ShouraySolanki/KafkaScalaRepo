package com.kafka.example

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.time.Duration
import java.util

class DatabaseConsumer {

  val consumerExample = new CreateConsumer
  val kafkaConsumer: KafkaConsumer[String,String] = consumerExample.consumetodatabase
  def consumeSend(): Unit ={
    kafkaConsumer.subscribe(util.Arrays.asList("jsontest1"))


    while (true){
      val records = kafkaConsumer.poll(Duration.ZERO)

      records.forEach{ rec =>
        try {

          val objectMapper = new ObjectMapper() with ScalaObjectMapper
          objectMapper.registerModule(DefaultScalaModule)
          val message = rec.value()

          val sum: Sum = objectMapper.readValue[Sum](message)
//          println(sum.getA())
//          println(sum.getB())
          val avg = (sum.getA()+sum.getB()+sum.getSum())/3

          ConnectDb.sendData(sum.getA(), sum.getB(), sum.getSum(),avg )
        }
        catch{
          case e:JsonProcessingException => throw e
        }


      }


    }

  }
  }

