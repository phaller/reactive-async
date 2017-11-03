import org.scalatest.FunSuite

import scala.concurrent.{ Promise, Await }
import scala.concurrent.duration._

import cell.HandlerPool

class PoolSuite extends FunSuite {
  test("onQuiescent") {
    val pool = new HandlerPool

    var i = 0
    while (i < 10000) {
      val p1 = Promise[Boolean]()
      val p2 = Promise[Boolean]()
      pool.execute { () => { p1.success(true) }: Unit }
      pool.onQuiescent { () => p2.success(true) }
      try {
        Await.result(p2.future, 1.seconds)
      } catch {
        case t: Throwable =>
          assert(false, s"failure after $i iterations")
      }
      i += 1
    }

    pool.shutdown()
  }
}
