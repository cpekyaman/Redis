package nosql.redis.inaction.web

import nosql.redis.inaction.core.{RedisNamespace,RedisService}
import RedisNamespace._

//TODO: implement watches, return correct boolean result 
object OnlineMarket {    
    def addItem(itemId: String, userId: Long, price: Double): Boolean = {
        val client = RedisService.getClient
        
        val inventory = userInventory(userId)
        val marketKey = s"${itemId}:${userId}"
        
        var retryCount = 0
        while(retryCount < 5) {
            try {
                val optResult = client.pipeline { pipe =>
                    //pipe.watch(inventory)
                    if(! pipe.sismember(inventory, itemId)) {
                        //pipe.unwatch()
                    } else {
                        pipe.zadd(marketNs, price, marketKey)
                        pipe.srem(inventory, itemId)
                    }           
                }                
            } catch {
                case e: Exception => 
                    println("got exception")  
                    retryCount += 1
            }
        }  
        
        true
    }
    
    def purchaseItem(buyerId: Long, sellerId: Long, itemId: String, lprice: Double): Boolean = {
        val client = RedisService.getClient
    
        val buyer = userWithNs(buyerId)
        val seller = userWithNs(sellerId)
        val marketKey = s"${itemId}:${sellerId}"
        val inventory = userInventory(buyerId)
        
        var retryCount = 0
        while(retryCount < 5) {
            try {
                val optResult = client.pipeline { pipe =>
                    //pipe.watch(List(buyer, marketNs))
                    
                    val price = pipe.zscore(marketNs, marketKey)
                    val funds = pipe.hget(buyer, "funds")
                    price match {
                        case Some(p) if(funds.exists{ _.toDouble > p }) =>
                            pipe.hincrby(seller, "funds", p.toInt)
                            pipe.hincrby(buyer, "funds", (-1 * p).toInt)
                            
                            pipe.sadd(inventory, itemId)
                            pipe.zrem(marketNs, marketKey)
                        case _ => println("fail")
                    }           
                }                
            } catch {
                case e: Exception => 
                    println("got exception")  
                    retryCount += 1
            }
        }
        
        true
    }
}
