package tw.com.chttl.spark.mllib.util

import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.FPGrowthModel

/**
 * Created by leorick on 2016/1/21.
 */
object FPGUtil extends Serializable {
  /**
   * usage: 計算 FPGModel 內 itemset 的 lift 值
   * return Array[(lift:Double, items:(Array[String], freq:Long, txCnt:Long, indProb:Double, itsetProb:Double, lift:Double))]
   * lift: itsetProb / indProb
   * items: frequent itemset
   * freq: frequent occurence of itemset
   * txCnt: total transactions in model
   * indProb: independent probability of each item
   * itsetProb: freq / txCnt
   * lift: itsetProb / indProb
   */
  def getFpModelLift(sc:SparkContext, txCnt:Long, fpModel:FPGrowthModel[String]) = {
    // 取得單一item的出現機率
    val fpItMap = fpModel.freqItemsets.filter{
      itset => itset.items.length == 1
    }.map{
      itset => (itset.items(0), itset.freq.toDouble/txCnt.toDouble)
    }.collectAsMap()
    val fpItBc = sc.broadcast(fpItMap)
    // 取得freqitemset的lift
    val fpmodelwLift = fpModel.freqItemsets.mapPartitions{ ite =>
      val fpItMap = fpItBc.value
      ite.filter{
        itset => itset.items.length > 1
      }.map{ itset =>
        val indProb = itset.items.map{
          item => fpItMap.getOrElse(item, 1.0D)
        }.toList.reduce( (a,b) => a * b )
        val itsetProb = itset.freq.toDouble/txCnt.toDouble
        ( itset.items, itset.freq, txCnt, indProb, itsetProb, itsetProb/indProb )
      }
      // 基於lift值排序
    }.map{
      case ( items, freq, txCnt, indProb, itsetProb, lift ) => (lift, ( items, freq, txCnt, indProb, itsetProb, lift ))
    }.sortByKey(false)
    fpmodelwLift
  }
}
