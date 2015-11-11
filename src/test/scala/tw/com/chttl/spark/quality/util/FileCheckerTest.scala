package tw.com.chttl.spark.quality.util

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.fs.Path
/**
 * Created by leorick on 2015/11/11.
 */
object FileCheckerTest {
  val appName = "FileCheckerTest"
  def checkTest() = {
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)
    val dfsPath = "file:///home/leo/dataset"
    val ret = FileChecker.check(sc, dfsPath)
    ret.foreach{ case (path, stat) => println(f"[${path}]:[${stat.toString}]")}
    /*
$ ll nolr.txt               # 1024*1024*1024*2=2147483648
-rw-rw-r-- 1 leo leo 2147483648 2015-11-11 07:22 nolr.txt
$ echo -n "0123456789" >> nolr.txt
$ echo  "0123456789" >> nolr.txt
$ ls -l ~/dataset/nolr.txt  # 2147483648+21=2147483669
-rw-rw-r-- 1 leo leo 2147483669 2015-11-11 07:23 nolr.txt

[file:/home/leo/dataset/sample_linear_regression_data.txt]:[stats: (count: 1, mean: 0.000000, stdev: 0.000000, max: 0.000000, min: 0.000000), NaN: 0]
[file:/home/leo/dataset/nolr1K.txt]:[stats: (count: 1, mean: 0.000000, stdev: 0.000000, max: 0.000000, min: 0.000000), NaN: 0]
[file:/home/leo/dataset/mknolr.sh]:[stats: (count: 1, mean: 0.000000, stdev: 0.000000, max: 0.000000, min: 0.000000), NaN: 0]
[file:/home/leo/dataset/women.csv]:[stats: (count: 1, mean: 0.000000, stdev: 0.000000, max: 0.000000, min: 0.000000), NaN: 0]
[file:/home/leo/dataset/nolr1M.txt]:[stats: (count: 1, mean: 0.000000, stdev: 0.000000, max: 0.000000, min: 0.000000), NaN: 0]
[file:/home/leo/dataset/nolr512.txt]:[stats: (count: 1, mean: 0.000000, stdev: 0.000000, max: 0.000000, min: 0.000000), NaN: 0]
[file:/home/leo/dataset/nolr1G.txt]:[stats: (count: 1, mean: 1073741824.000000, stdev: 0.000000, max: 1073741824.000000, min: 1073741824.000000), NaN: 0]
[file:/home/leo/dataset/nolr.txt]:[stats: (count: 1, mean: 2147483648.000000, stdev: 0.000000, max: 2147483648.000000, min: 2147483648.000000), NaN: 0]     */
  }
}
