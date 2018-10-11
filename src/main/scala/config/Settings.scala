package config

import com.typesafe.config.ConfigFactory



object Settings {

  private val config = ConfigFactory.load()

  object WebLogGen {
    private val weblogGen = config.getConfig("settings")

    lazy val sparkW: String = weblogGen.getString("spark")
    lazy val HadoopHome: String = weblogGen.getString("hadoop")
    lazy val checkPointDirAurora: String = weblogGen.getString("checkPointAurora")
    lazy val kafkaServersAWS: String = weblogGen.getString("kafkaServersAWS")
    lazy val kafkaServersDocker:String = weblogGen.getString("kafkaServersDocker")
    lazy val kafkaServersLocal:String = weblogGen.getString("kafkaServersLocal")
    lazy val topics :String = weblogGen.getString("topics")
    lazy val mysqlURL:String = weblogGen.getString("mysqlURL")
    lazy val mysqlURLLocal:String = weblogGen.getString("mysqlURLLocal")
    lazy val mysqlUsername:String = weblogGen.getString("mysqlUsername")
    lazy val mysqlPassword:String = weblogGen.getString("mysqlPassword")
    lazy val aWSAuroraURL:String = weblogGen.getString("aWSAuroraURL")
    lazy val aWSUser:String = weblogGen.getString("aWSUser")
    lazy val aWSPassword:String = weblogGen.getString("aWSPassword")
  }

}
