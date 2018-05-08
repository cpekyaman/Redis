package nosql.redis.inaction.web

import nosql.redis.inaction.core.{RedisNamespace,RedisService}
import RedisNamespace._

object ShoppingCart {    
    def addItem(session: String, item: String, count: Integer) {
        val conn = RedisService.getClient
        if(count <= 0) 
            conn.hdel(cart(session), item) 
        else 
            conn.hset(cart(session), item, count)
    }
}
