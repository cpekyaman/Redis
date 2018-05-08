package nosql.redis.inaction.core

import com.redis._

object RedisService {
    private lazy val client = new RedisClient("localhost", 6379)
	def getClient = client
}
