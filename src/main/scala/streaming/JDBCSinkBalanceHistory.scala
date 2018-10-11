package streaming

import java.sql.{Connection, DriverManager, Statement}

import org.apache.spark.sql.{ForeachWriter, Row}

class JDBCSinkBalanceHistory(url:String, username:String, password:String) extends ForeachWriter[Row]{
  val driver = "com.mysql.jdbc.Driver"
  var connection:Connection = _
  var statement:Statement = _

  def open(partitionId: Long,version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url,username,password)
    statement = connection.createStatement
    true
  }
  def process(value: Row): Unit = {

    statement.executeUpdate("insert into balance_history(time,user_id,asset,business,`change`,balance,detail,btype) values("
      + "'" + value.getDouble(1) + "'" + ","//time
      + "'" + value.getInt(2) + "'" + ","//user_id
      + "'" + value.getString(3) + "'" + ","//asset
      + "'" + value.getString(4) + "'" + ","//business
      + "'" + value.getDecimal(5) + "'" + ","//change
      + "'" + value.getDecimal(6) + "'" + ","//balance
      + "'" + value.getString(7) + "'" + ","//detail
      + "'" + value.getString(8) + "'"//btype
      + ")")
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.close
  }
}