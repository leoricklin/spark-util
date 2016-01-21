package tw.com.chttl.spark.quality.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by leorick on 2015/12/2.
 */
object NAStatTest {
  val appName = "NAStatTest"
  val sparkConf = new SparkConf().setAppName(appName)
  val sc = new SparkContext(sparkConf)

  def statsWithMissingTest() = {
    val rdd: RDD[Array[Double]] = sc.parallelize(Seq(Array(1.0, 2.0), Array(2.0,3.0)), 2)
    NAStat.statsWithMissing(rdd)
    /*
    res7: Array[NAStatCounter] = Array(
    stats: (count: 2, mean: 1.500000, stdev: 0.500000, max: 2.000000, min: 1.000000), NaN: 0,
    stats: (count: 2, mean: 2.500000, stdev: 0.500000, max: 3.000000, min: 2.000000), NaN: 0)

     */
  }
}
