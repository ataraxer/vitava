package ottla

import akka.util.ByteString
import kafka.api._
import java.nio.{ByteBuffer, ByteOrder}


object KafkaAPI {
  import RequestKeys._

  def encodeRequest(request: RequestOrResponse): ByteString = {
    val buffer = ByteBuffer.allocate(request.sizeInBytes + 2)
    buffer.putShort(request.requestId.get)
    request.writeTo(buffer)
    buffer.rewind()
    ByteString(buffer)
  }

  def decodeResponse(response: ByteString): RequestOrResponse = {
    val buffer = response.toByteBuffer
    val key = buffer.getShort
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

  def zipWithKey(key: Short, response: ByteString): ByteString = {
    implicit val order = ByteOrder.BIG_ENDIAN
    val keyPrefix = ByteString.newBuilder.putShort(key)
    val result = keyPrefix.result ++ response
    result
  }
}

