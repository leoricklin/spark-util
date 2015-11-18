package tw.com.chttl.spark.core.util

/**
 * Created by leorick on 2015/11/11.
 */
object StringHelperTest {
  def tokenizeTest() = {
    val str = "a b c \n d e"
    val ret1 = StringHelper.tokenize(str, " ", false)
    assert(ret1.toSeq equals Seq("a","b","c","\n","d","e"))
    val ret2 = StringHelper.tokenize(str, """\W+""")
    assert(ret2.toSeq equals Seq("a","b","c","d","e"))
  }
}
