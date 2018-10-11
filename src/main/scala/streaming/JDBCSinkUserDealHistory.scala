package streaming

import java.sql.{Connection, DriverManager, Statement}

import org.apache.spark.sql.{ForeachWriter, Row}

class JDBCSinkUserDealHistory(url:String, username:String, password:String) extends ForeachWriter[Row]{
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

    statement.executeUpdate("insert into user_deal_history(time,user_id,deal_user_id,market,deal_id,order_id,deal_order_id,side,role,price,amount,deal,fee,deal_fee,platform,stock,deal_stock) values("

      + "'" + value.getDouble(1) + "'" + ","//time
      + "'" + value.getInt(2)+ "'"  + "," //user_id
      + "'" + value.getInt(3) + "'" + ","//deal_user_id
      + "'" + value.getString(4) + "'" + "," //market
      + "'" + value.getDecimal(5) + "'" + ","//deal_id
      + "'" + value.getDecimal(6) + "'" + ","//order_id
      + "'" + value.getDecimal(7) + "'" + "," //deal_order_id
      + "'" + value.getInt(8) + "'" + ","//side
      + "'" + value.getInt(9) + "'" + "," //role
      + "'" + value.getDecimal(10) + "'" + ","//price
      + "'" + value.getDecimal(11) + "'" + ","//amount
      + "'" + value.getDecimal(12) + "'" + "," //deal
      + "'" + value.getDecimal(13) + "'" + ","//fee
      + "'" + value.getDecimal(14) + "'" + "," //deal_fee
      + "'" + value.getString(15) + "'" + ","//platform
      + "'" + value.getString(16) + "'" + "," //stock
      + "'" + value.getString(17) + "'"//deal_stock
      + ")")
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.close
  }
}