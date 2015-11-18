package tw.com.chttl.spark.core.util

/**
 * Created by leorick on 2015/11/12.
 */
object IntParaTest {
  def unapplyTest() = {
    val IntPara(v1) = "12"
    assert(v1 == 12)
  }
}
