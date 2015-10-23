package ottla

import akka.actor.ActorSystem
import akka.stream.io.Framing
import akka.stream.scaladsl._
import akka.util.ByteString

import kafka.message.{Message => KafkaMessage, _}
import kafka.api._
import kafka.common.{TopicAndPartition => Partition, _}


object Kafka {
  import KafkaApi._

  case class Message(key: ByteString, data: ByteString)

  object Message {
    def apply(key: String, data: String): Message = {
      Message(ByteString(key), ByteString(data))
    }
  }

  val protocol = {
    val kafkaApi = CorrelatedBidiFlow(encodeRequest _, decodeResponse _)
    val framing = Framing.simpleFramingProtocol(Int.MaxValue - 4)
    kafkaApi atop framing
  }


  def wrapMessage
      (partition: Partition, clientId: String, correlationId: () => Int)
      (message: Message) = {

    import scala.collection.mutable

    val data = new KafkaMessage(
      key = message.key.toArray[Byte],
      bytes = message.data.toArray[Byte],
      codec = NoCompressionCodec,
      payloadOffset = 0,
      payloadSize = message.data.size)

    new ProducerRequest(
      correlationId(),
      clientId = clientId,
      requiredAcks = -1.toShort,
      ackTimeoutMs = 8 * 1000,
      data = mutable.Map(partition -> new ByteBufferMessageSet(data)))
  }


  def extractAck(partition: Partition)(response: ProducerResponse): Long = {
    val status = response.status(partition)

    status.error match {
      case ErrorMapping.NoError => status.offset
      case code => throw ErrorMapping.exceptionFor(code)
    }
  }


  def fetchRequest
      (partition: Partition, clientId: String, correlationId: () => Int)
      (offset: Long): FetchRequest = {

    new FetchRequest(
      correlationId(),
      clientId,
      maxWait = 500,
      minBytes = 64 * 1024,
      requestInfo = Map(partition -> PartitionFetchInfo(offset, 64 * 1024)))
  }


  def extractMessage
      (partition: Partition)
      (response: FetchResponse): Seq[Message] = {

    val data = response.data(partition)

    data.error match {
      case ErrorMapping.NoError =>
        val result = {
          for (MessageAndOffset(message, offset) <- data.messages) yield {
            val key = ByteString(message.key)
            val payload = ByteString(message.payload)
            Message(key, payload)
          }
        }

        result.toSeq
      case code => throw ErrorMapping.exceptionFor(code)
    }
  }
}


case class Kafka(host: String, port: Int = 9092) {
  import Kafka._

  def outgoingConnection()(implicit system: ActorSystem) = {
    val connection = Tcp().outgoingConnection(host, port)
    Kafka.protocol join connection
  }

  def producerFlow(clientId: String, topic: String) = {
    // TODO: choose partition
    val partition = Partition(topic, 0)
    val correlationId = Iterator.from(0).next _
    val inbound = wrapMessage(partition, clientId, correlationId) _
    val outbound = extractAck(partition) _
    BidiFlow(inbound, outbound)
  }

  def producer(clientId: String, topic: String)(implicit system: ActorSystem) = {
    val connection = outgoingConnection()
    val flow = producerFlow(clientId, topic)
    flow join connection.collect { case response: ProducerResponse => response }
  }

  def consumerFlow(clientId: String, topic: String) = {
    // TODO: choose partition
    val partition = Partition(topic, 0)
    val correlationId = Iterator.from(0).next _
    val inbound = fetchRequest(partition, clientId, correlationId) _
    val outbound = extractMessage(partition) _
    BidiFlow(inbound, outbound)
  }

  def consumer(clientId: String, topic: String)(implicit system: ActorSystem) = {
    val connection = outgoingConnection()
    val pipeline = connection.collect { case response: FetchResponse => response }
    val flow = consumerFlow(clientId, topic)
    val result = flow join pipeline
    result.mapConcat( _.to[collection.immutable.Iterable] )
  }
}

