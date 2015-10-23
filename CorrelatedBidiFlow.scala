package ottla

import akka.stream._
import akka.stream.scaladsl._


object CorrelatedBidiFlow {
  import FlowGraph.Implicits._

  def apply[I1, O1, I2, O2, K](encoder: I1 => O1, decoder: (I1, I2) => O2) = {
    BidiFlow() { implicit builder =>
      val bcast = builder add Broadcast[I1](2)
      val encode = builder add Flow[I1].map(encoder)
      val zipKey = builder add ZipWith(decoder)

      bcast ~> encode
      bcast ~> zipKey.in0

      BidiShape(bcast.in, encode.outlet, zipKey.in1, zipKey.out)
    }
  }
}

