package ottla

import akka.util.ByteString

import scodec.Codec
import scodec.codecs._
import scodec.bits.BitVector

import shapeless.Typeable


sealed trait KafkaMessage
sealed trait KafkaRequest extends KafkaMessage
sealed trait KafkaResponse extends KafkaMessage

case class MetadataRequest(
    correlationId: Int,
    clientId: String,
    topics: Seq[String])
  extends KafkaRequest

case class MetadataResponse(
    correlationId: Int,
    brokers: Seq[Broker],
    metadata: Seq[TopicMetadata])
  extends KafkaResponse

case class Broker(id: Int, host: String, port: Int)

case class TopicMetadata(
    error: Int,
    topic: String,
    metadata: Seq[PartitionMetadata])

case class PartitionMetadata(
    error: Int,
    id: Int,
    leader: Int,
    replicas: Seq[Int],
    isr: Seq[Int])


object KafkaMessage {
  import kafka.api.RequestKeys._

  val string = variableSizeBytes(int16, utf8)
  val bytes = variableSizeBytes(int32, byte)

  def array[T: Typeable](codec: Codec[T]) = {
    listOfN(int32, codec).upcast[Seq[T]]
  }

  def key(value: Int) = int16.unit(value)
  val version = int16.unit(0)

  val error = int16

  val correlationId, partitionId, brokerId, port = int32
  val replicas, isr = array(brokerId)
  def leader = brokerId
  val topic, clientId, host = string
  val topics = array(topic)

  def request(key: Int) = this.key(key) :: version :: correlationId :: clientId

  def metadataRequestCodec = (request(MetadataKey) :+ topics).as[MetadataRequest]

  def metadataResponseCodec = {
    (correlationId :: array(broker) :: array(topicMeta)).as[MetadataResponse]
  }

  def broker = (brokerId :: host :: port).as[Broker]

  def topicMeta = (error :: topic :: array(partitionMeta)).as[TopicMetadata]
  def partitionMeta = (error :: partitionId :: leader :: replicas :: isr).as[PartitionMetadata]

  val requestCodec = metadataRequestCodec.upcast[KafkaRequest]
  val responseCodec = metadataResponseCodec.upcast[KafkaResponse]
}


object KafkaRequest {
  val codec = KafkaMessage.requestCodec

  def encode(message: KafkaRequest): ByteString = {
    ByteString(codec.encode(message).require.toByteBuffer)
  }

  def decode(data: ByteString): KafkaRequest = {
    codec.decode(BitVector(data.toByteBuffer)).require.value
  }
}


object KafkaResponse extends {
  val codec = KafkaMessage.responseCodec

  def encode(message: KafkaResponse): ByteString = {
    ByteString(codec.encode(message).require.toByteBuffer)
  }

  def decode(data: ByteString): KafkaResponse = {
    codec.decode(BitVector(data.toByteBuffer)).require.value
  }
}

