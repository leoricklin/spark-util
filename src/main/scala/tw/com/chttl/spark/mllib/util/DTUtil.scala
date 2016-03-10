package tw.com.chttl.spark.mllib.util

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, Node}
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable

/**
 * Created by leorick on 2016/2/18.
 */
object DTUtil extends Serializable  {

  /**
   *
   * @param node
   * @return String of node, e.g.: id=1, F4, imp=82.72535, cond=0.66800
   */
  def printNode(node: Node): String = {
    if (node.isLeaf) {
      f"id=${node.id}, pred=${node.predict}"
    } else {
      val condition = node.split.get.featureType.toString match {
        case "Continuous" => f"${node.split.get.threshold}%5.5f"
        case _ => node.split.get.categories.map{cat => f"${cat}%3.1f"}.mkString("[",",","]")
      }
      f"id=${node.id}, F${node.split.get.feature}, imp=${node.impurity}%5.5f, cond=${condition}"
    }
  }

  /**
   * helper function for dTree2Array()
   * @param ary
   * @param node
   * @return array of nodes
   */
  def putNode(ary:Array[Node], node:Node): Array[Node] = {
    ary(node.id-1) = node
    if (node.isLeaf) {
      //
    } else {
      if (!node.leftNode.isEmpty) { putNode(ary, node.leftNode.get) }
      if (!node.rightNode.isEmpty){ putNode(ary, node.rightNode.get) }
      //
    }
    ary
  }

  /**
    * transform nodes in Tree to nodes in array, the index of array is the id of node - 1,
    * e.g.: Array(0) is node with id = 1
    * @param tree
    * @param sorted
    * @return array of nodes
    */
  def dTree2Array(tree:DecisionTreeModel, sorted:Boolean = false): Array[Node] = {
    /*
    val size = (for (i <- 0 to tree.depth) yield {
      java.lang.Math.pow(2,i)
    }).toArray.sum.toInt
    */
    val size = (java.lang.Math.pow(2,tree.depth+1) -1).toInt
    val ary = new Array[Node](size)
    if (sorted) {
      putNode(ary, tree.topNode).
        sortBy { node =>
          if (node == null || node.stats.isEmpty) { 0 }
          else { node.stats.get.gain } }
    } else {
      putNode(ary, tree.topNode)
    }
  }

  /**
   * compute the avg. gain of features with Nodes in Array[Node]. it excludes the leaf nodes which do'nt have the feature and impurity attributes.
   * @param nodes
   * @return Map[k, v] with key = feature id, value = (number of nodes , avg. Gain)
   */
  def nodeAryAvgGain(nodes: Array[Node]): Map[Int, (Int, Double)] = {
    val ret: Map[Int, (Int, Double)] = nodes.
      filter{ node => (node != null && !node.split.isEmpty && !node.stats.isEmpty) }.
      groupBy{ node =>
        if (node.split.isDefined)
          node.split.get.feature
        else
          (-1) }.
      mapValues{ nodes =>
        val avg = nodes.map{ node => if (node.stats.isDefined) node.stats.get.gain else 0 }.sum / nodes.size
        (nodes.size, avg) }
    ret
  }

  /**
   * compute the avg. gain of features with Nodes in Array[DecisionTreeModel]
   * @param trees
   * @return map of (feature_id, (number_of_nodes, avg_gain))
   */
  def nodeTreeAvgGain(trees: Array[DecisionTreeModel]): Map[Int, (Int, Double)] = {
    val ret: Map[Int, (Int, Double)] = ( trees.map{ tree => nodeAryAvgGain( dTree2Array(tree) ) } ).
      reduce{ (map1, map2) =>  map1 ++ map2.
        map{ case (id2:Int, (cnt2:Int, avg2:Double)) =>
          val (cnt1:Int, avg1:Double) = map1.getOrElse(id2, 0 -> 0.0)
          val cnt = cnt1+cnt2
          id2 -> (cnt, ((cnt1*avg1+cnt2*avg2).toDouble)/cnt ) } }
    ret
  }

  /**
   * selecting feature using RF Classification and sort by information gain of each feature
   * @param lps: RDD of LabeledPoint
   * @param numClasses: number of target classes
   * @param numIteration: number of evaluation iteration
   * @param minFeatureOccurRatio: ratio of feature occurrences in subtree
   * @param numTrees: number of subtrees in Random Forrest
   * @param featureSubset: feature subset strategy
   * @param impurity: impurity
   * @param depth: number of deepth for subtree
   * @param bins: max bins for subtree
   * @param seed: random seed
   * @param sorted: if sort the result
   * @return Seq of (feature_id, (feature_occurs_count, avg_info_gain))
   */
  def selFeaturesbyRfClassifier( lps: RDD[LabeledPoint] , numClasses: Int
                      , numIteration:Int = 3 , minFeatureOccurRatio:Double = 0.2
                      , numTrees:Int = 10, featureSubset:String = "auto", impurity:String = "entropy", depth:Int = 4, bins:Int = 100
                      , seed:Int = 100 , sorted:Boolean = true): Seq[(Int, (Int, Double))] = {
    lps.cache()
    // 每個feature在tree出現最少次數
    val minFeatureOccurs = BigDecimal(((java.lang.Math.pow(2, depth+1) -1) * minFeatureOccurRatio)).
      setScale(0, scala.math.BigDecimal.RoundingMode.UP).toInt // 7
    val featureSelects: Map[Int, (Int, Double)] = (1 to numIteration).
        map{ i =>
        val modelRf = RandomForest.trainClassifier(lps, numClasses, Map[Int,Int]()
          , numTrees, featureSubset, impurity, depth, bins, seed)
        // this metric cloud be used for model selection
        /*
        val precision = (new MulticlassMetrics( lps.
          zip( modelRf.predict( lps.map{ lp => lp.features }) ).
          map{ case (lp: LabeledPoint, predict: Double) => (lp.label, predict) } ) ).precision
         */
        val nodeAvgGain: Map[Int, (Int, Double)] = nodeTreeAvgGain(modelRf.trees)
        nodeAvgGain.filter{ case (k, (cnt, gain)) => cnt.toString.toInt >= minFeatureOccurs }  }.
        reduce{ (map1, map2) =>
        map1 ++  map2.
          map{ case (id2:Int, (cnt2:Int, avg2:Double)) =>
          val (cnt1:Int, avg1:Double) = map1.getOrElse(id2, 0 -> 0.0)
          val cnt = cnt1+cnt2
          id2 -> (cnt, ((cnt1*avg1+cnt2*avg2).toDouble)/cnt ) } }
    lps.unpersist(true)
    if (sorted) {
      featureSelects.toSeq.sortBy{ case (k, (cnt, gain)) => gain }.reverse
    } else {
      featureSelects.toSeq
    }
  }
}
