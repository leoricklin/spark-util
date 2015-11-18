package tw.com.chttl.spark.sql.util

/**
 * Created by leorick on 2015/11/18.
 */
object SqlHelperTest {
 def main (args: Array[String]) {
   val driverName="com.mysql.jdbc.Driver"
   val url = "jdbc:mysql://tf2p076.cht.local:3306/leo"
   val username = "leo"
   val password = "qazWSX"
   Class.forName(driverName).newInstance
   val conn = SqlHelper.getConn(driverName, url, username, password)
   val stmt = SqlHelper.getStmt(conn)
   val sql="select count(*) from v_turdr_sum1_app_5"
   val ret = SqlHelper.getResult(stmt, sql)
   SqlHelper.printResult(ret)

 }
}
