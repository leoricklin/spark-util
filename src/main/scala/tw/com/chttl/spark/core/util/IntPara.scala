package tw.com.chttl.spark.core.util

/**
 * Created by leorick on 2015/8/10.
 */
object IntPara extends Serializable {
  def unapply(str:String) = {
    try {
      Some(str.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }
}
