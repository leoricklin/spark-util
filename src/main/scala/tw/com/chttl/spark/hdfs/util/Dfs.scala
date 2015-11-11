package tw.com.chttl.spark.hdfs.util

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkContext

/**
 * Created by leorick on 2015/11/4.
 */
object Dfs extends Serializable {
  /**
   * Calculate the space usage in bytes under dfsPath
   * @param sc: org.apache.spark.SparkContext
   * @param dfsPath: org.apache.hadoop.fs.Path
   * @return Bytes occupied by the dfsPath
   */
  def countUsage(sc: SparkContext, dfsPath: Path): Long = {
    val itemStats: Array[FileStatus] = dfsPath.getFileSystem(sc.hadoopConfiguration).listStatus(dfsPath)
    val filesUsage: Long = itemStats.filter(_.isFile).map(file => file.getLen).sum
    var ret = filesUsage
    if (itemStats.exists(_.isDirectory)) {
      val subDirsUsage = itemStats.filter(_.isDirectory).map{ dir => countUsage(sc, dir.getPath) }.sum
      ret += subDirsUsage
    }
    ret
  }
  /**
   * Find FileStatus objects satified the filter condition
   * @param sc: org.apache.spark.SparkContext
   * @param dfsPath: org.apache.hadoop.fs.Path
   * @param filter: (FileStatus => Boolean)
   * @return Array of org.apache.hadoop.fs.FileStatus
   */
  def find(sc: SparkContext, dfsPath: Path, filter: (FileStatus => Boolean) ): Array[FileStatus] = {
    val itemStats: Array[FileStatus] = dfsPath.getFileSystem(sc.hadoopConfiguration).listStatus(dfsPath)
    val hereFiltered: Array[FileStatus] = itemStats.filter(filter)
    var ret = hereFiltered
    if (itemStats.exists(_.isDirectory)) {
      val subDirFiltered: Array[FileStatus] = ( itemStats.filter(_.isDirectory).map{ dir => find(sc, dir.getPath, filter) }
        ).reduce{(ary1, ary2) => ary1++ary2}
      ret = ret ++ subDirFiltered
    }
    ret
  }
  /**
   * Likes hdfs dfs -ls, returns stat on the FileStatus objects with the following format:
   *  permissions userid groupid filesize filename
   * @param fileStatus: Array[org.apache.hadoop.fs.FileStatus]
   * @return Array of Array of String
   */
  def ls(fileStatus: Array[FileStatus]): Array[Array[String]] = {
    val out: Array[Array[String]] = fileStatus.map{ file =>
      val perm = ( if (file.isDirectory) "d" else "-" )+ file.getPermission.toString
      val owner = file.getOwner
      val group = file.getGroup
      val len = file.getLen.toString
      val name = file.getPath.getName
      Array(perm,owner,group,len,name)
    }
    val maxWidth: Array[Int] = out.map{ case Array(perm,owner,group,len,name) =>
      Array(owner.length, group.length, len.length, name.length)
    }.reduce{(ary1, ary2) => Array(
      if (ary1(0)>ary2(0)) ary1(0) else ary2(0)
      ,if (ary1(1)>ary2(1)) ary1(1) else ary2(1)
      ,if (ary1(2)>ary2(2)) ary1(2) else ary2(2)
      ,if (ary1(3)>ary2(3)) ary1(3) else ary2(3))
    }
    val formated = out.map{ case Array(perm,owner,group,len,name) =>
      Array("%s " format perm
        , "%" + maxWidth(0) + "s" format owner
        , "%" + maxWidth(1) + "s" format group
        , "%" + maxWidth(2) + "s" format len
        , "%" + maxWidth(3) + "s" format name)
    }
    formated
  }

}
