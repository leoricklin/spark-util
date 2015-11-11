package tw.com.chttl.spark.core.util

/**
 * Created by leorick on 2015/8/10.
 */
object StringHelper extends Serializable {
  def tokenize(src : String, delimiter : String, trim: Boolean = false) = {
    if (trim)
      src.split(delimiter).map(_.trim)
    else
      src.split(delimiter)
  }
  def replaceChar(ary : Array[String], src: String, tar: String) = {
    ary.map( it =>
      if (it == src)
        tar
      else
        it
    )
  }

}
