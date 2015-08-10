package tw.com.chttl.spark.core.util
import org.apache.spark.rdd._
/**
 * Created by leorick on 2015/8/10.
 */
package tw.com.chttl.spark.core.util

object NAStat extends Serializable {

  def statsWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {
    val nastats = rdd.mapPartitions( (iter: Iterator[Array[Double]]) => {
      val cnts: Array[NAStatCounter] = iter.next().map(d => NAStatCounter(d))
      iter.foreach(arr => {
        cnts.zip(arr).foreach { case (cnt, d) => cnt.add(d) }
      })
      Iterator(nas)
    })
    nastats.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    })
  }
}

