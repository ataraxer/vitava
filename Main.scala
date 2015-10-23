package ottla

import akka.stream.scaladsl.Source


object KafkaCat extends StreamApp {
  val clientId = "foobar"
  val topic = "topic"

  val kafka = Kafka(scala.sys.process.Process("boot2docker ip").!!.trim)
  val kafkaProducer = kafka.producer(clientId, topic)
  val kafkaConsumer = kafka.consumer(clientId, topic)
  val kafkaCat = kafkaProducer via kafkaConsumer

  val message = Kafka.Message(
    key = "tag:poetry",
    data = "The quick brown fox jumps over the lazy dog.")

  Source.single(message)
    .via(kafkaCat)
    .map( _.data.utf8String )
    .runForeach(println)
}


object KafkaConversation extends StreamApp {
  val kafka = Kafka(scala.sys.process.Process("boot2docker ip").!!.trim)
  val topic = "topic"

  val messages = Vector(
    Kafka.metadataRequest(topic),
    Kafka.offsetsRequest(topic))

  Source(messages)
    .via(kafka.outgoingConnection())
    .recover(errorHandler)
    .map(KafkaApi.formatResponse)
    .runForeach(println)
}

