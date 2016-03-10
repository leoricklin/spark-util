package tw.com.chttl.spark.quality.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, FixedLengthInputFormat}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD, NewHadoopRDD}
import tw.com.chttl.spark.mllib.util.NAStatCounter
import scala.collection.mutable.ArrayBuffer

/**
 * Created by leorick on 2015/11/11.
 */
object FileChecker extends Serializable {

  /**
   *
   * @param sc
   * @param dfsPath
   * @param fixLength
   * @param delimiter
   * @return
   */
  def check(sc:SparkContext, dfsPath: String, fixLength: Int = 33554432, delimiter:String = """\n+"""): RDD[(String, NAStatCounter)] = {
    val hdpconf = sc.hadoopConfiguration
    hdpconf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, fixLength)
    val rdds: NewHadoopRDD[LongWritable, BytesWritable] = ( (sc.
      newAPIHadoopFile[LongWritable, BytesWritable, FixedLengthInputFormat](dfsPath)).
      asInstanceOf[NewHadoopRDD[LongWritable, BytesWritable]] )
    val recordLenOfFile: RDD[(String, NAStatCounter)] = rdds.mapPartitionsWithInputSplit{ case (split, ite) =>
      val fsplit: FileSplit = split.asInstanceOf[FileSplit]
      val recordLens = ArrayBuffer[Array[Long]]()
      try {
        while (ite.hasNext) {
          val item: (LongWritable, BytesWritable) = ite.next()
          recordLens += ( (new String(item._2.copyBytes())).split(delimiter).map{ record => record.length.toLong } )
        }
      } catch {
        case e:Exception =>
      }
      // because blocks are sequential reading from the same split, here combines the last part of previous block with the first part of next block as a complete record
      val recordLenOfSplit: Array[Long] = recordLens.size match {
        case 0 => Array(0L)
        case 1 => recordLens(0)
        case _ => recordLens.reduce{ (ary1,ary2) => ((ary1.init) :+ (ary1.last+ary2.head)) ++ (ary2.tail) }
      }
      Iterator((fsplit.getPath.toString, fsplit.getStart, recordLenOfSplit))
    }.groupBy{ case (path, offset, recordLenOfSplit) =>
      // grouping splits of same file
      path
    }.map{ case (path, ite) =>
      // sort and combine splits in the same file
      val recordLenOfFile: Array[Long] = ite.size match {
        case 1 => ite.head._3
        case _ => ite.toArray.sortBy{ case (path, offset, recordLenOfSplit) => offset
          }.map{ case (path, offset, recordLenOfSplit) => recordLenOfSplit
          }.reduce{ (ary1,ary2) => ((ary1.init) :+ (ary1.last+ary2.head)) ++ (ary2.tail) }
      }
      (path, recordLenOfFile.map{ length => NAStatCounter(length)}.reduce( (n1,n2) => n1.merge(n2) ) )
    }
    recordLenOfFile
  }
}
