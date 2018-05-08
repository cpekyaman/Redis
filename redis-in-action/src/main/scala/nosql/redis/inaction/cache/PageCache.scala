package nosql.redis.inaction.cache

import nosql.redis.inaction.core.{RedisNamespace,RedisService}
import RedisNamespace._
import com.redis.RedisClient
import RedisClient._

object PageCache {
    def get(request: Any, callback: Any => Option[String]): Option[String] = {
        val conn = RedisService.getClient
        if(! canCache_?(conn, request)) {
            callback(request)
        } else {
            val pageKey = cacheKey(hashRequest(request))
            var content = conn.get(pageKey)
            if(! content.isDefined) {
                content = callback(request)
                conn.setex(pageKey, 300, content.getOrElse(""))
            }
            content
        }
    }
    
    def rescale() {
        val conn = RedisService.getClient
        conn.zremrangebyrank(viewedNs, 20000, -1)
        conn.zinterstoreWeighted(viewedNs, List((viewedNs, 0.5D)), MAX)
    }
    
    private def canCache_?(conn: RedisClient, request: Any): Boolean = {
        val itemId = getItemId(request)
        itemId match {
            case Some(id) if(! isDynamic_?(request)) => 
                val rank = conn.zrank(viewedNs, id)
                rank match {
                    case Some(rval) if rval < 10000 => true
                    case _ => false
                }
            case _ => false
        }    
    }
    
    private def hashRequest(request: Any): String = request.toString
    
    private def getItemId(request: Any): Option[Long] = Some(1L) 
    
    private def isDynamic_?(request: Any): Boolean = false
}
