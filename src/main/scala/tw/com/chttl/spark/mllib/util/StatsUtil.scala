package tw.com.chttl.spark.mllib.util

import java.io.Serializable

import org.apache.spark.mllib.linalg.{Matrix, Vector}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.{HashMap}

/**
 * Created by leorick on 2016/2/22.
 */
object StatsUtil extends Serializable {
  /**
   *
   * @param rdd
   * @return Statistics,
   * e.g.: mean=[1.2248814820915922E-4], min=[-6.135090144103502], max=[6.32781125594535], var=[1.2098790417283618]
   */
  def printStats(rdd:RDD[Vector]) = {
    val stats = Statistics.colStats(rdd)
    f"mean=${stats.mean}%s, min=${stats.min}%s, max=${stats.max}%s, var=${stats.variance}%s"
  }

  /**
   * @param matrix: correlation matrix of features, returned by Statistics.corr()
   * @param names: seq of variable names
   * @param width: width of display name
   * @return tabluar format of vectors coefficient matrix
   * e.g.:
          |1     |2     |3     |4     |5     |6     |7     |8     |9
    1     |+1.000|+0.564|-0.112|+0.275|-0.428|+0.673|-0.727|+0.281|-0.835
    2     |+0.564|+1.000|-0.381|+0.428|-0.327|+0.562|-0.587|+0.228|-0.489
    3     |-0.112|-0.381|+1.000|-0.427|-0.124|-0.163|-0.003|-0.171|-0.232
    4     |+0.275|+0.428|-0.427|+1.000|-0.094|+0.314|-0.220|+0.382|-0.050
    5     |-0.428|-0.327|-0.124|-0.094|+1.000|-0.641|+0.720|+0.115|+0.588
    6     |+0.673|+0.562|-0.163|+0.314|-0.641|+1.000|-0.763|-0.058|-0.675
    7     |-0.727|-0.587|-0.003|-0.220|+0.720|-0.763|+1.000|-0.091|+0.821
    8     |+0.281|+0.228|-0.171|+0.382|+0.115|-0.058|-0.091|+1.000|-0.016
    9     |-0.835|-0.489|-0.232|-0.050|+0.588|-0.675|+0.821|-0.016|+1.000
   */
  def corrMatrixTable(matrix:Matrix, names:Option[Seq[String]] = None, width:Int = 6): String = {
    val rows = matrix.numRows
    val cols = matrix.numCols
    val ary = matrix.toArray
    val fmtTitle = "%-"+ width + "." +width + "s"
    val titles = names.getOrElse( (1 to cols).map{_.toString} ).
      map( id => fmtTitle format(id) )
    val coefs: IndexedSeq[Array[String]] = (0 until rows).
      map{ idx =>
      ary.slice(idx*cols, (idx*cols)+cols).
        map{ (coef: Double) => f"${coef}%+-1.3f" }.
        map{ str => fmtTitle format(str) } }
    val out = f"${" "*width}|${titles.mkString("|")}\n" + {
      for (i <- 0 until titles.size)
      yield { f"${titles(i)}|${coefs(i).map(v => f"${v}").mkString("|")}" }
    }.mkString("\n")
    out
  }

  /**
   * @param matrix: correlation matrix of features, returned by Statistics.corr()
   * @param names: seq of variable names
   * @return sorted sequence of (variable-name-pair, coefficient)
   * e.g.:
[1-9][-0.835]
[7-9][+0.821]
...
...
[4-8][+0.382]
[2-3][-0.381]
..
[8-9][-0.016]
[3-7][-0.003]
   */
  def sortCorrMatrix(matrix:Matrix, names:Option[Seq[String]] = None) = {
    val rows = matrix.numRows
    val cols = matrix.numCols
    val ary: Array[Double] = matrix.toArray
    val titles = names.getOrElse( (1 to cols).map{_.toString} )
    val map:HashMap[String, Double] = new HashMap()
    for (i <- 0 until (rows -1); j <- (i+1) until cols )
    {  map+=((f"${titles(i)}-${titles(j)}", ary(cols*i+j))) }
    map.toSeq.sortBy{ case (k, v) => -java.lang.Math.abs(v) }
  }

}
