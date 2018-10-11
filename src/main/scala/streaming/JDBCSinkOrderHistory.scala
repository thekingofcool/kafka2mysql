package streaming

import java.sql.{Connection, DriverManager, Statement}

import org.apache.spark.sql.{ForeachWriter, Row}

class JDBCSinkOrderHistory(url:String, username:String, password:String) extends ForeachWriter[Row]{
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

    statement.executeUpdate("insert into order_history(create_time,finish_time,user_id,market,source,t,side,price,amount,taker_fee,maker_fee,deal_stock,deal_money,deal_fee,platform) values("

      + "'" + value.getDouble(1) + "'" + ","//create_time
      + "'" + value.getDouble(2) + "'" + "," //finish_time
      + "'" + value.getInt(3) + "'" + ","//user_id
      + "'" + value.getString(4) + "'" + "," //market
      + "'" + value.getString(5) + "'" + ","//source
      + "'" + value.getInt(6) + "'" + ","//t
      + "'" + value.getInt(7) + "'" + "," //side
      + "'" + value.getDecimal(8) + "'" + ","//price
      + "'" + value.getDecimal(9) + "'" + "," //amount
      + "'" + value.getDecimal(10) + "'" + ","//taker_fee
      + "'" + value.getDecimal(11) + "'" + ","//maker_fee
      + "'" + value.getDecimal(12) + "'" + "," //deal_stock
      + "'" + value.getDecimal(13) + "'" + ","//deal_money
      + "'" + value.getDecimal(14) + "'" + "," //deal_fee
      + "'" + value.getString(15) + "'"//platform
      + ")")
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.close
  }
}