package ottla


trait StreamUtils {
  def errorHandler[T] = PartialFunction[Throwable, T] {
    case error => println(error); throw error
  }

  def checker[T] = CorrelatedBidiFlow(identity[T], (_: T) == (_: T))
}

