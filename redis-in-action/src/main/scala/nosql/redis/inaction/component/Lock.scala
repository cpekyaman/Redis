package nosql.redis.inaction.component

import nosql.redis.inaction.core.RedisService
import com.redis.RedisCommand

object Lock {
	def acquire(conn: Option[_ <: RedisCommand], name: String, timeout: Int = 10): Option[String] = {
		val client = conn getOrElse RedisService.getClient
		val identifier = java.util.UUID.randomUUID().toString
		if(client.setnx(s"lock:$name", identifier)) {
		    Some(identifier)
		} else {
		    None
		}
	}
	
	def acquireWithTimeout(name: String, timeout: Int = 10): Boolean = {
	    val client = RedisService.getClient
		val identifier = java.util.UUID.randomUUID().toString
		
		if(client.setnx(s"lock:$name", identifier)) {
		    client.expire(s"lock:$name", timeout)
		    return true
		} 
		
		if(! client.ttl(s"lock:$name").isDefined) {
		    client.expire(s"lock:$name", timeout)		   
		}
		false
	}
	
	def release(name: String, identifier: String) {
		val client = RedisService.getClient
		client.pipeline { pipe =>
		    pipe.get(s"lock:$name") match {
		        case Some(id) if(id.equals(identifier)) => 
		            pipe.del(s"lock:$name")
		    }
		}
	}
}
