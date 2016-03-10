package tw.com.chttl.spark.mllib.util

import org.apache.spark.mllib.linalg.Vector

/**
 * Created by leorick on 2016/2/19.
 */
object LRUtil extends Serializable {
  /**
   *
   * @param coef coefficient of LRModel
   * @param names array of variable name
   * @param sort if sorted by coef
   * @return LR model formulation, e.g.: (+0.99987) * X0 + (+1.99975) * X1 + (+0.00043) * X2
   */
  def printLrModel(coef: Vector, names:Option[Array[String]] = None, sort:Boolean = false): String = {
    val name: Array[String] = names.getOrElse{ (for (i <- 0 until coef.size) yield {f"X${i}%s"}).toArray }
    val pair = coef.toArray.zip(name)
    val ret = if(sort) { coef.toArray.zip(name).sortBy{ case (coef,name) => java.lang.Math.abs(coef)  }.reverse
    } else {
      coef.toArray.zip(name)
    }
    ret.map{ case (coef, name) => f"(${coef}%+8.5f) * ${name}" }.mkString(" + ")
  }
}
