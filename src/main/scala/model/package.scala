
package object model {

  case class BalanceHistory(id:BigInt,time:Double,user_id:Int,asset:String,business:String,change:BigDecimal,balance:BigDecimal,detail:String,btype:String) extends Serializable

  case class OrderHistory(id:BigInt,create_time:Double,finish_time:Double,user_id:Int,market:String,source:String,t:Int,side:Int,price:BigDecimal,amount:BigDecimal,taker_fee:BigDecimal,maker_fee:BigDecimal,deal_stock:BigDecimal,deal_money:BigDecimal,deal_fee:BigDecimal,platform:String) extends Serializable

  case class UserDealHistory(id:BigInt,time:Double,user_id:Int,deal_user_id:Int,market:String,deal_id:BigInt,order_id:BigInt,deal_order_id:BigInt,side:Int,role:Int,price:BigDecimal,amount:BigDecimal,deal:BigDecimal,fee:BigDecimal,deal_fee:BigDecimal,platform:String,stock:String,deal_stock:String) extends Serializable

}
