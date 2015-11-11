package tw.com.chttl.spark.hdfs.util

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by leorick on 2015/11/4.
 */
object DfsTest {
  val appName = "DfsTest"

  def countUsageTest(): Unit = {
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)
    val path = "hdfs:///user/leoricklin"
    val dfsPath = new Path(path)
    val ret1 = Dfs.countUsage(sc, dfsPath)
    /*
[leoricklin@tf2p076 lib]$ hdfs dfs -du -s /user/leoricklin
283676982489  849631315431  /user/leoricklin

val ret1 = countUsage(hdpconf, dfsPath)
ret1: Long = 283676982489
     */
  }

  def findTest(): Unit = {
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)
    val path = "hdfs:///user/leoricklin"
    val dfsPath = new Path(path)
    val ret2 = Dfs.find(sc, dfsPath, {(filestat: FileStatus) => filestat.getLen > 4*1024*1024*1024L})
    val ret3 = Dfs.ls(ret2)
    /*
scala> ret3.foreach(ary => println(ary.mkString(" ")))
-rw-rw----  leoricklin leoricklin 4810314082 result-n-20150615-20150617101402-00000.gz
-rw-rw----  leoricklin leoricklin 4616552472 result-n-20150615-20150617101402-00001.gz
-rw-rw----  leoricklin leoricklin 4421654331 result-n-20150615-20150617101402-00002.gz
     */
  }
}
