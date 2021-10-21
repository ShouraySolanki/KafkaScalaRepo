package com.kafka.example


import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties


class ProducerExample {
  val properties = new Properties
  properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  val producer = new KafkaProducer[String, String](properties)


  def produceMethod(topic: String, message: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, message))
  }
}

