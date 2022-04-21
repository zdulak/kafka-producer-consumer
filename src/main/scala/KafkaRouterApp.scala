import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.time.Duration
import java.util.Properties
import java.util.regex.Pattern
import scala.jdk.CollectionConverters._

object KafkaRouterApp extends App {
  val outputProps = new Properties()
  outputProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  outputProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  outputProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val inputProps = new Properties()
  inputProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  inputProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  inputProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  inputProps.put(ConsumerConfig.GROUP_ID_CONFIG, "input")

  val inputConsumer = new KafkaConsumer[String, String](inputProps)
  inputConsumer.subscribe(Pattern.compile("topic-.*"))

  val outputProducer = new KafkaProducer[String, String](outputProps)

  while (true) {
    val records = inputConsumer.poll(Duration.ofMillis(100)).asScala
    records.foreach { record =>
      outputProducer.send(new ProducerRecord[String, String](record.value, record.key, "hello"))
      println(record.key(), record.value())
    }
  }
}
