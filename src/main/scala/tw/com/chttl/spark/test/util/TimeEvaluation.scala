package tw.com.chttl.spark.test.util

/**
 * Created by leo on 2015/8/24.
 */
object TimeEvaluation extends Serializable {
  def time[A](f: => A) = {
    val s = System.nanoTime
    val ret = f
    println("time: "+(System.nanoTime-s)/1e6+"ms")
    ret
  }
}
