package streaming

import config.Settings.WebLogGen
import model.{BalanceHistory, OrderHistory, UserDealHistory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql._
import utils.AuroraUtils.getSparkSession


object Kafka2Mysql {
  def main(args: Array[String]): Unit = {
    //获取sparkSession
    val spark :SparkSession = getSparkSession
    //导入隐式转换

    import spark.implicits._
    //读取kafka数据流
    val dfInput: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", WebLogGen.kafkaServersAWS)
      .option("subscribe", WebLogGen.topics)
      //.option("startingOffsets","latest")
      .option("startingOffsets","earliest")
      .load()

    val contentDF:Dataset[String] = dfInput.selectExpr("CAST(value AS STRING)").as[String]



    //定义字符串正则规则
    val sqlPattern = """[\s\S]+INSERT\sINTO\s[\s\S]+\([\s\S]+\)[\s\S]+VALUES\s\([\s\S]+\);?""".r
    val balance_historySqlPattern = """INSERT\sINTO\sbalance_history[\s\S]+VALUES[\s\S]+""".r
    val order_historySqlPattern = """INSERT\sINTO\sorder_history[\s\S]+VALUES[\s\S]+""".r
    val user_deal_historySqlPattern = """INSERT\sINTO\suser_deal_history[\s\S]+VALUES[\s\S]+""".r

    //按照正则规则过滤出符合sql条件的数据集，并对这些数据集进行字段截取。
    val sqlDS: Dataset[String] = contentDF
      .filter(x => sqlPattern.pattern.matcher(x).matches())
      .map(sql => sql.substring(sql.indexOf("INSERT")))


<!--
      //根据sql语句组成的Dataset中，依次按照三个表的正则规则分别过滤，形成插入各表的Dataset[Table]
    val insertBHTable:Dataset[BalanceHistory] = sqlDS.filter(x => balance_historySqlPattern.pattern.matcher(x).matches())
      .map(sql => {
        val value = sql.substring(sql.indexOf("(", sql.indexOf("VALUES")) + 1, sql.length - 2)
        val valueArr: Array[String] = value.split(",").map(_.trim)
        val balanceHistory = BalanceHistory(null, valueArr(0).toDouble, valueArr(1).toInt, valueArr(2).substring(1,valueArr(2).length-1), valueArr(3).substring(1,valueArr(3).length-1), BigDecimal(valueArr(4)), BigDecimal(valueArr(5)), sql.substring(sql.indexOf("{"),sql.indexOf("}")+1), valueArr(valueArr.length-1).substring(1,valueArr(valueArr.length-1).length))
        balanceHistory
      })

    val insertOHTable:Dataset[OrderHistory] = sqlDS.filter(x => order_historySqlPattern.pattern.matcher(x).matches())
      .map(sql =>{
        val value = sql.substring(sql.indexOf("(", sql.indexOf("VALUES")) + 1, sql.length - 2)
        val valueArr: Array[String] = value.split(",").map(_.trim)
        val orderHistory: OrderHistory = OrderHistory(null,valueArr(1).toDouble,valueArr(2).toDouble, valueArr(3).toInt, valueArr(4).substring(1,valueArr(4).length-1), valueArr(5).substring(1,valueArr(5).length-1), valueArr(6).toInt, valueArr(7).toInt, BigDecimal(valueArr(8)), BigDecimal(valueArr(9)), BigDecimal(valueArr(10)), BigDecimal(valueArr(11)), BigDecimal(valueArr(12)), BigDecimal(valueArr(13)), BigDecimal(valueArr(14)), valueArr(15).substring(1,valueArr(15).length-1))
        orderHistory
      })
    val insertUDHTable:Dataset[UserDealHistory] = sqlDS.filter(x => user_deal_historySqlPattern.pattern.matcher(x).matches())
      .map(sql => {
        val value = sql.substring(sql.indexOf("(", sql.indexOf("VALUES")) + 1, sql.length - 2)
        val valueArr: Array[String] = value.split(",").map(_.trim)
        val userDealHistory: UserDealHistory = UserDealHistory(null, valueArr(0).toDouble, valueArr(1).toInt, valueArr(2).toInt, valueArr(3).substring(1,valueArr(3).length-1), BigInt(valueArr(4)), BigInt(valueArr(5)), BigInt(valueArr(6)), valueArr(7).toInt, valueArr(8).toInt, BigDecimal(valueArr(9)), BigDecimal(valueArr(10)), BigDecimal(valueArr(11)), BigDecimal(valueArr(12)), BigDecimal(valueArr(13)), valueArr(14).substring(1,valueArr(14).length-1), valueArr(15).substring(1,valueArr(15).length-1), valueArr(16).substring(1,valueArr(16).length-1))
        userDealHistory
      })
-->



    implicit val match1Error = org.apache.spark.sql.Encoders.kryo[Dataset[BalanceHistory]]
    implicit val match2Error = org.apache.spark.sql.Encoders.kryo[Dataset[OrderHistory]]
    implicit val match3Error = org.apache.spark.sql.Encoders.kryo[Dataset[UserDealHistory]]


    val insertBHTable: Dataset[BalanceHistory] = sqlDS.rdd.filter(x => balance_historySqlPattern.pattern.matcher(x).matches())
      .map(statement => {
        val sqlArr: Array[String] = statement.replaceAll("INSERT", "|INSERT")
          .split("|").filterNot(_.isEmpty)
        val temp: Dataset[BalanceHistory] = spark.sparkContext.parallelize(sqlArr).toDS().map(sql => {
          val value = sql.substring(sql.indexOf("(", sql.indexOf("VALUES")) + 1, sql.length - 2)
          val valueArr: Array[String] = value.split(",").map(_.trim)
          val balanceHistory = BalanceHistory(null, valueArr(0).toDouble, valueArr(1).toInt, valueArr(2).substring(1, valueArr(2).length - 1), valueArr(3).substring(1, valueArr(3).length - 1), BigDecimal(valueArr(4)), BigDecimal(valueArr(5)), sql.substring(sql.indexOf("{"), sql.indexOf("}") + 1), valueArr(valueArr.length - 1).substring(1, valueArr(valueArr.length - 1).length))
          balanceHistory
        })
        temp
      }).reduce((x, y) => x.union(y))


    val insertOHTable:Dataset[OrderHistory] = sqlDS.rdd.filter(x => order_historySqlPattern.pattern.matcher(x).matches())
      .map(statement =>{
        val sqlArr:Array[String] = statement.replaceAll("INSERT","|INSERT")
          .split("|").filterNot(_.isEmpty)
        val temp:Dataset[OrderHistory] = spark.sparkContext.parallelize(sqlArr).toDS().map(sql =>{
          val value = sql.substring(sql.indexOf("(", sql.indexOf("VALUES")) + 1, sql.length - 2)
          val valueArr: Array[String] = value.split(",").map(_.trim)
          val orderHistory: OrderHistory = OrderHistory(null,valueArr(1).toDouble,valueArr(2).toDouble, valueArr(3).toInt, valueArr(4).substring(1,valueArr(4).length-1), valueArr(5).substring(1,valueArr(5).length-1), valueArr(6).toInt, valueArr(7).toInt, BigDecimal(valueArr(8)), BigDecimal(valueArr(9)), BigDecimal(valueArr(10)), BigDecimal(valueArr(11)), BigDecimal(valueArr(12)), BigDecimal(valueArr(13)), BigDecimal(valueArr(14)), valueArr(15).substring(1,valueArr(15).length-1))
          orderHistory
        })
        temp
      }).reduce((x,y)=>x.union(y))

    val insertUDHTable: Dataset[UserDealHistory] = sqlDS.rdd.filter(x => user_deal_historySqlPattern.pattern.matcher(x).matches())
      .map(statement => {
        val sqlArr: Array[String] = statement.replaceAll("INSERT", "|INSERT")
          .split("|").filterNot(_.isEmpty)
        val temp: Dataset[UserDealHistory] = spark.sparkContext.parallelize(sqlArr).toDS().map(sql => {
          val value = sql.substring(sql.indexOf("(", sql.indexOf("VALUES")) + 1, sql.length - 2)
          val valueArr: Array[String] = value.split(",").map(_.trim)
          val userDealHistory: UserDealHistory = UserDealHistory(null, valueArr(0).toDouble, valueArr(1).toInt, valueArr(2).toInt, valueArr(3).substring(1, valueArr(3).length - 1), BigInt(valueArr(4)), BigInt(valueArr(5)), BigInt(valueArr(6)), valueArr(7).toInt, valueArr(8).toInt, BigDecimal(valueArr(9)), BigDecimal(valueArr(10)), BigDecimal(valueArr(11)), BigDecimal(valueArr(12)), BigDecimal(valueArr(13)), valueArr(14).substring(1, valueArr(14).length - 1), valueArr(15).substring(1, valueArr(15).length - 1), valueArr(16).substring(1, valueArr(16).length - 1))
          userDealHistory
        })
        temp
      }).reduce((x, y) => x.union(y))



    //定义sink的目的地信息
    val url = WebLogGen.mysqlURLLocal
    val username = WebLogGen.mysqlUsername
    val password = WebLogGen.mysqlPassword

    //定义三种写入数据库的writer
    val writerBH =new JDBCSinkBalanceHistory(url,username,password)
    val writerOH = new JDBCSinkOrderHistory(url,username,password)
    val writerUDH = new JDBCSinkUserDealHistory(url,username,password)

    //定义query将BalanceHistory数据追加到表中
    val appendBHDF: DataFrame = insertBHTable
      .select($"id", $"time", $"user_id", $"asset", $"business", $"change", $"balance", $"detail", $"btype")

    val queryBH = appendBHDF.writeStream
      .foreach(writerBH)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(10000))
      .start()


    //定义query将OrderHistory数据追加到表中
    val appendOHDF:DataFrame = insertOHTable
      .select($"id",$"create_time",$"finish_time",$"user_id",$"market",$"source",$"t",$"side",$"price",$"amount",$"taker_fee",$"maker_fee",$"deal_stock",$"deal_money",$"deal_fee",$"platform")
    val queryOH = appendOHDF.writeStream
      .foreach(writerOH)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(10000))
      .start()

    //定义query将UserDealHistory数据追加到表中
    val appendUDHDF:DataFrame = insertUDHTable
      .select($"id",$"time",$"user_id",$"deal_user_id",$"market",$"deal_id",$"order_id",$"deal_order_id",$"side",$"role",$"price",$"amount",$"deal",$"fee",$"deal_fee",$"platform",$"stock",$"deal_stock")
    val queryUDH = appendUDHDF.writeStream
      .foreach(writerUDH)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(10000))
      .start()

    queryBH.awaitTermination()
    queryOH.awaitTermination()
    queryUDH.awaitTermination()
  }
}