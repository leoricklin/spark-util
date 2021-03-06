package tw.com.chttl.spark.hdfs.util

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by leorick on 2015/11/4.
 */
object DfsTest {
  val appName = "DfsTest"

  def countUsageTest(): Unit = {
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)
    // :load /home/leoricklin/jar/Dfs.scala
    val dfsPath = new Path("hdfs:///hive")
    val time = System.currentTimeMillis()
    val m1 = FileSystem.get(sc.hadoopConfiguration).listStatus(dfsPath).
      filter{_.isDirectory}.
      map{ status =>  status.getPath -> Dfs.countUsage(sc, status.getPath)  }.
      groupBy(_._1).mapValues{ v => v.head._2}
    var status = FileSystem.get(sc.hadoopConfiguration).listStatus(dfsPath).
      filter{_.isDirectory}
    val path = status.head.getPath
    val pathusage = scala.collection.mutable.Map( path -> Dfs.countUsage(sc, path) )
    status.foreach{ status =>
      val path = status.getPath
      pathusage += (path -> Dfs.countUsage(sc, path)) }

    val m2 = FileSystem.get(sc.hadoopConfiguration).listStatus(dfsPath).
      filter{_.isDirectory}.
      map{ status =>  status.getPath -> Dfs.countUsage(sc, status.getPath)  }.
      groupBy(_._1).mapValues{ v => v.head._2}
    m2.map{ case (path, len) =>
      ( path, len - m1(path) ) }.toArray.sortBy{ case (path, diff) => diff }.
      reverse.foreach{ case (path, diff) => println(f"${path.getName}, ${diff}}") }

    val a1: Iterator[Int] = Array(1,2,3).toIterator
    val v = a1.next
    val m3 = scala.collection.mutable.Map( v -> v.toString())
    a1.foreach{ v => m3 += (v -> v.toString)  }

    Dfs.countUsage(sc, dfsPath)
    /*
ret1: Long = 283676982489

[leoricklin@tf2p076 lib]$ hdfs dfs -du -s /user/leoricklin
283676982489  849631315431  /user/leoricklin
     */
  }

  def findTest(): Unit = {
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)
    // :load /home/leoricklin/jar/Dfs.scala
    Dfs.ls{
      Dfs.find(sc, new Path("hdfs:///user/leoricklin"), {(filestat: FileStatus) => filestat.getLen > 4*1024*1024*1024L})
    }.foreach(ary => println(ary.mkString(" ")))
    /*
-rw-rw----  leoricklin leoricklin 4810314082 2015/9/3 16:50:28 2015/11/12 11:29:3 result-n-20150615-20150617101402-00000.gz
-rw-rw----  leoricklin leoricklin 4616552472 2015/9/3 16:51:40  2015/9/3 16:50:28 result-n-20150615-20150617101402-00001.gz
-rw-rw----  leoricklin leoricklin 4421654331 2015/9/3 16:52:48  2015/9/3 16:51:40 result-n-20150615-20150617101402-00002.gz
     */
    Dfs.ls {
      Dfs.find(sc, new Path("hdfs:///user/leoricklin"), { (filestat: FileStatus) =>
        (filestat.getAccessTime > 0) && (filestat.getAccessTime > (System.currentTimeMillis() - 2*60*60*1000))
      })
    }.foreach(ary => println(ary.mkString(" ")))
    /*
-rw-r--r--  leoricklin leoricklin 527 2015/10/12 14:28:58 2015/12/4 11:24:54 workflow.xml
-rw-rw----  leoricklin leoricklin 425 2015/11/12 11:16:45 2015/12/4 11:51:46   hdfs-du.sh
     */
    val tt = {
      val dfs = FileSystem.get(sc.hadoopConfiguration)
      // before cat file
      Dfs.ls{ dfs.listStatus(new Path("hdfs:///user/leoricklin/sh/hdfs-du.sh"))
      }.foreach(ary => println(ary.mkString(" ")))
      /*
-rw-rw----  leoricklin leoricklin 425 2015/11/12 11:16:45 2015/11/12 11:16:45 hdfs-du.sh
      // after cat file
$ hdfs dfs -cat /user/leoricklin/sh/hdfs-du.sh
       */
      Dfs.ls{ dfs.listStatus(new Path("hdfs:///user/leoricklin/sh/hdfs-du.sh"))
      }.foreach(ary => println(ary.mkString(" ")))
      /*
-rw-rw----  leoricklin leoricklin 425 2015/11/12 11:16:45 2015/12/4 11:51:46 hdfs-du.sh
       */
      dfs.listStatus(new Path("hdfs:///user/leoricklin/sh/hdfs-du.sh")).head.getAccessTime
      // = 1449201106211, 2015/12/4 11:51:46
      System.currentTimeMillis()
      // = 1449201116000, 2015/12/4 11:51:56
    }
  }
}
