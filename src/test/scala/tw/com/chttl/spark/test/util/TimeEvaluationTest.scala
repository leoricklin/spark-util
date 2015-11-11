package tw.com.chttl.spark.test.util

/**
 * Created by leorick on 2015/11/4.
 */
object TimeEvaluationTest {
  def timeTest(args: Array[String]) {
    val ret = TimeEvaluation.time(Seq(1 to 1000000000))
    /*
time: 0.125557ms
ret: Seq[scala.collection.immutable.Range.Inclusive] = List(Range(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,..
     */
  }
}
