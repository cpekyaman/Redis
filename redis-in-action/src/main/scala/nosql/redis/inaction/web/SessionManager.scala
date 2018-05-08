package nosql.redis.inaction.web

import nosql.redis.inaction.core.{RedisNamespace,RedisService}
import RedisNamespace._
import scala.concurrent.Await
import scala.concurrent.duration._

object SessionManager {    
    val sessionCountLimit = 100000
    
    def checkToken(token: String): String = RedisService.getClient.hget(loginNs, token) getOrElse ""
    
    def updateToken(token: String, userId: Long, item: Option[Long] = None) {
        val conn = RedisService.getClient
        val now = System.currentTimeMillis
        
        conn.hset(loginNs, token, userWithNs(userId))
        conn.zadd(recentNs, now, token)
        
        if(item.isDefined) {
            conn.zadd(viewedWithNs(token), now, itemWithNs(item.get))
            conn.zremrangebyrank(viewedWithNs(token), 0, -26)
            conn.zincrby(viewedNs, -1, item)
        }
    }
    
    def updateTokenPipelined(token: String, userId: Long, item: Option[Long] = None) {
        val conn = RedisService.getClient
        val now = System.currentTimeMillis
        var commands = List(
            {() => conn.hset(loginNs, token, userWithNs(userId))},
            {() => conn.zadd(recentNs, now, token)}
        )
        if(item.isDefined) {
            commands = commands ::: List(
                {() => conn.zadd(viewedWithNs(token), now, itemWithNs(item.get)) },
                {() => conn.zremrangebyrank(viewedWithNs(token), 0, -26) },
                {() => conn.zincrby(viewedNs, -1, item) }
            )
        }
        
        val promises = conn.pipelineNoMulti(commands)
        val results = promises.map{ a => Await.result(a.future, 200.milliseconds) }
    }
    
    def cleanSessions() {
        val conn = RedisService.getClient
        val size = conn.zcard(recentNs).getOrElse(0L)
        if(size >= sessionCountLimit) {
            val endIndex = (size - sessionCountLimit).min(100).toInt
            val tokens = conn.zrange(recentNs, 0, endIndex - 1).getOrElse(Nil)
            
            var sessionDataKeys = List[String]()
            for(token <- tokens) {
                sessionDataKeys = viewedWithNs(token) :: sessionDataKeys
                sessionDataKeys = cart(token) :: sessionDataKeys
            }
            
            conn.del(sessionDataKeys.head, sessionDataKeys.tail:_*)
            conn.hdel(loginNs, tokens.head, tokens.tail:_*)
            conn.zrem(recentNs, tokens.head, tokens.tail:_*)
        }
    }
}
