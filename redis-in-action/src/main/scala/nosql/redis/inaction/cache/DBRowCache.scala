package nosql.redis.inaction.cache

import nosql.redis.inaction.core.{RedisNamespace,RedisService}
import nosql.redis.inaction.domain.Inventory
import RedisNamespace._
import com.redis.RedisClient

object DBRowCache {
        
    def scheduleRow(rowId: String, delay: Integer) {
        val conn = RedisService.getClient
        conn.zadd(delayNs, delay.toDouble, rowId)
        conn.zadd(scheduleNs, System.currentTimeMillis, rowId)
    }
    
    def cacheRows() {
        val conn = RedisService.getClient        
        val scheduled = conn.zrangeWithScore[String](scheduleNs, 0,0).getOrElse(List[(String, Double)]())
        process(conn, scheduled)        
    }
    
    def process(conn: RedisClient, scheduled: List[(String, Double)]) {
        val now = System.currentTimeMillis
        scheduled match {
            case (rowId, score) :: xs if score < now =>
                val delay = conn.zscore(delayNs, rowId) getOrElse 0.0D
                if(delay <= 0) {
                    conn.zrem(delayNs, rowId)
                    conn.zrem(scheduleNs, rowId)
                    conn.del(inventoryKey(rowId))
                }
                
                val dbRow = Inventory.get(rowId)
                conn.zadd(scheduleNs, now + delay, rowId)
                conn.set(inventoryKey(rowId), Inventory.toJson(dbRow))
                
                process(conn, xs)
            case _ =>
        }
    }
}
