package com.kafka.example

import scala.collection
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util

class DataTransformer{
  val objectMapper = new ObjectMapper

  val producerExample = new ProducerExample

  val consumerExample = new ConsumerExample
  val kafkaConsumer: KafkaConsumer[String,String] = consumerExample.consumeMethod

  def getSendTransform(): String = {

    kafkaConsumer.subscribe(util.Arrays.asList("jsontest"))
    kafkaConsumer.poll(0)
    kafkaConsumer.seekToBeginning(kafkaConsumer.assignment())

    while (true){
      val records = kafkaConsumer.poll(Duration.ZERO)

      for ( rec <- records){

        val message= rec.v
      }

    }


  }
}