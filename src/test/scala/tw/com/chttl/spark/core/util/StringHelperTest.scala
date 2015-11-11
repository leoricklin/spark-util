package tw.com.chttl.spark.core.util

/**
 * Created by leorick on 2015/11/11.
 */
object StringHelperTest {
  def tokenizeTest() = {
    val str = "a b c \n d e"
    val ret1 = StringHelper.tokenize(str, " ")
    /*
ret1: Array[String] =
Array(a, b, c, "
", d, e)
     */
    val ret2 = StringHelper.tokenize(str, """\W+""")
    /*
ret2: Array[String] = Array(a, b, c, d, e)
     */
  }
}
