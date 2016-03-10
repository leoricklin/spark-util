package tw.com.chttl.spark.mllib.util

import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{Vectors, Matrix, Matrices}

/**
 * Created by leorick on 2016/3/9.
 */
object StatsUtilTest {
  def testCorrMatrixTable(sc:SparkContext) = {
    val vcs = sc.parallelize(Seq(Array(1.0, 2.0), Array(3.0, 4.0), Array(5.0, 6.0))).
      map{ ary => Vectors.dense(ary)}
    val mat = Statistics.corr(vcs)
    println(StatsUtil.corrMatrixTable(mat))
    /*
      |1     |2
1     |+1.000|+1.000
2     |+1.000|+1.000
     */
  }

  def testSortCorrMatrix(sc:SparkContext) = {
    val vcs = sc.parallelize(Seq(Array(1.0, 2.0), Array(3.0, 4.0), Array(5.0, 6.0))).
      map{ ary => Vectors.dense(ary)}
    val mat = Statistics.corr(vcs)
    println(StatsUtil.sortCorrMatrix(mat).map{ case (k,v) => f"[${k}][${v}%+-1.3f]" }.mkString("\n"))
    /*
ArrayBuffer((1-2,1.0))
     */
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("")
    val sc = new SparkContext(sparkConf)

  }
}
