package utils

import java.lang.management.ManagementFactory

import config.Settings.WebLogGen
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AuroraUtils {
  val isIDE: Boolean = {
    //IDE中运行
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }

  def getSparkSession: SparkSession = {
    //获取sparkConf
    val conf = new SparkConf()
      .setAppName("Kafka2Mysql")
    val checkpointDirectory = WebLogGen.checkPointDirAurora
    //IDE中运行
    if (isIDE) {
      //System.setProperty("hadoop.home.dir", WebLogGen.HadoopHome)
      conf.setMaster("local[*]")
    }
    //初始化sparkSession
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir(checkpointDirectory)

    spark
  }

}
