package ottla

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer


trait StreamApp extends StreamUtils with App {
  implicit val system = ActorSystem("ottla")
  implicit val flowBuilder = ActorMaterializer()
  implicit val executor = system.dispatcher
}

