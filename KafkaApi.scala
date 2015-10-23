package ottla

import akka.util.ByteString
import kafka.api._
import java.nio.{ByteBuffer, ByteOrder}


object KafkaApi {
  import RequestKeys._

  def encodeRequest(request: RequestOrResponse) = {
    val buffer = ByteBuffer.allocate(request.sizeInBytes + 2)
    buffer.putShort(request.requestId.get)
    request.writeTo(buffer)
    buffer.rewind()
    ByteString(buffer)
  }

  def decodeResponse(request: RequestOrResponse, response: ByteString) = {
    val key = request.requestId.get
    val buffer = response.toByteBuffer
    decoderForKey(key)(buffer)
  }

  val decoderForKey = Map[Short, ByteBuffer => RequestOrResponse](
    ProduceKey -> ProducerResponse.readFrom,
    FetchKey -> FetchResponse.readFrom,
    OffsetsKey -> OffsetResponse.readFrom,
    MetadataKey -> TopicMetadataResponse.readFrom,
    LeaderAndIsrKey -> LeaderAndIsrResponse.readFrom,
    StopReplicaKey -> StopReplicaResponse.readFrom,
    UpdateMetadataKey -> UpdateMetadataResponse.readFrom,
    OffsetCommitKey -> OffsetCommitResponse.readFrom,
    OffsetFetchKey -> OffsetFetchResponse.readFrom,
    ConsumerMetadataKey -> ConsumerMetadataResponse.readFrom)

  def formatResponse(response: RequestOrResponse): String = {
    import kafka.api._
    import kafka.cluster.Broker
    import kafka.common.{TopicAndPartition, ErrorMapping}
    import kafka.message._

    response match {
      case TopicMetadataResponse(brokers, topicsMeta, correlationId) => {
        val result = new StringBuilder
        result append f"MetadataResponse for $correlationId:\n"
        result append "  Brokers:\n"
        for (Broker(id, host, port) <- brokers) {
          result append f"    $id at $host:$port\n"
        }
        result append "  Partitions:\n"
        for {
          TopicMetadata(topic, partitionsMeta, error) <- topicsMeta
          PartitionMetadata(id, leader, replicas, isr, error) <- partitionsMeta
        } {
          val partition = f"$topic:$id"
          val leaderString = leader match {
            case Some(broker) => f"at broker ${broker.id}"
            case None => "without leader"
          }
          result append f"    $partition $leaderString\n"
        }
        result.toString
      }

      case OffsetResponse(correlationId, data) => {
        val result = new StringBuilder
        result append f"OffsetResponse for $correlationId:\n"
        for ((origin, PartitionOffsetsResponse(error, offsets)) <- data) {
          val TopicAndPartition(topic, id) = origin
          val partition = f"$topic:$id"
          val offsetsString = offsets match {
            case Seq() => "none"
            case Seq(one) => one.toString
            case range => f"${range.head}..${range.last}"
          }
          result append f"  $partition -> $offsetsString\n"
        }
        result.toString
      }

      case ProducerResponse(correlationId, status) => {
        val result = new StringBuilder
        result append f"ProducerResponse for $correlationId:\n"
        for ((origin, ProducerResponseStatus(error, offset)) <- status) {
          val TopicAndPartition(topic, id) = origin
          val partition = f"$topic:$id"
          val errorString = error match {
            case ErrorMapping.NoError => "successful"
            case code => ErrorMapping.exceptionFor(code).getMessage
          }
          result append f"  $partition -> $errorString for $offset\n"
        }
        result.toString
      }

      case FetchResponse(correlationId, data) => {
        val result = new StringBuilder
        result append f"FetchResponse for $correlationId:\n"
        for ((origin, FetchResponsePartitionData(error, hw, messages)) <- data) {
          val TopicAndPartition(topic, id) = origin
          val partition = f"$topic:$id"
          val errorString = error match {
            case ErrorMapping.NoError => "successful"
            case code => ErrorMapping.exceptionFor(code).getMessage
          }
          result append f"  $partition is $errorString:\n"
          for (MessageAndOffset(message, offset) <- messages) {
            val content = ByteString(message.payload).utf8String
            result append f"    $offset -> $content\n"
          }
        }
        result.toString
      }

      case other => other.toString
    }
  }
}

