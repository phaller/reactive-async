import org.scalatest.FunSuite

import java.util.concurrent.CountDownLatch

import scala.util.{Success, Failure}
import scala.concurrent.ExecutionContext.Implicits.global

class BaseSuite extends FunSuite {
  test("putFinal") {
    val latch = new CountDownLatch(1)

    val completer = CellCompleter[String, Int]("somekey")
    val cell = completer.cell
    cell.onComplete {
      case Success(v) =>
        assert(v === 5)
        latch.countDown()
      case Failure(e) =>
        assert(false)
    }
    completer.putFinal(5)

    latch.await()
  }
}
