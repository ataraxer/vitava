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

}

