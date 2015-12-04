package tw.com.chttl.spark.quality.util

import org.apache.spark.util.StatCounter

/**
 * Created by leorick on 2015/8/10.
 */

class NAStatCounter extends Serializable {
  val stats: StatCounter = new StatCounter()
  var missing: Long = 0
  def add(x: Double): NAStatCounter = {
    if (java.lang.Double.isNaN(x)) {
      missing += 1
    } else {
      stats.merge(x)
    }
    this
  }
  def merge(other: NAStatCounter): NAStatCounter = {
    stats.merge(other.stats)
    missing += other.missing
    this
  }
  override def toString = {
    f"stats: ${stats.toString}, NaN: ${missing}"
  }
}

object NAStatCounter extends Serializable {
  def apply(x: Double) = new NAStatCounter().add(x)

  def na2Zero(d: Double) = if (Double.NaN.equals(d)) 0.0 else d
}

