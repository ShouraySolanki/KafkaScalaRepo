package com.kafka.example


import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import com.kafka.example.util.KafkaUtils


class ProducerExample {
  val kafkaUtils = new KafkaUtils
  val producer: Producer[_,_] = kafkaUtils.getProducer : Producer[_,_]


  def produceMethod(topic: String, message: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, message))
  }
}

