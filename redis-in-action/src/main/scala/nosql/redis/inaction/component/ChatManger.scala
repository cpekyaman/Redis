package nosql.redis.inaction.component

import nosql.redis.inaction.core.RedisService
import com.redis.RedisCommand

object ChatManger {
	def create(sender: String, recipients: List[String], message: String, chatId: Option[Long]): Boolean = {
	    val conn = RedisService.getClient
	    val cid = chatId orElse conn.incr("id:chat")
	    
	    cid match {
	        case Some(id) =>
	            val recpWithScore = recipients.map( r => (0.0, r) ).toIndexedSeq
	            conn.pipeline { pipe =>
	                pipe.zadd(s"chat:$id", 0.0, sender, recpWithScore:_*)
	                for(rec <- recipients) {
	                    pipe.zadd(s"seen:$rec", 0.0, id)
	                }	                
	            }	 
	            return sendMessage(Some(conn), id, sender, message)
	        case None =>
	            println("could not create chat")
	            false
	    }
	}
	
	def sendMessage(conn: Option[_ <: RedisCommand], chatId: Long, sender: String, message: String): Boolean = {
	    val client = conn getOrElse RedisService.getClient
	    val lockId = Lock.acquire(Some(client), s"chat:$chatId")
	    
	    lockId match {
	        case Some(lid) =>
	            try {	            
                    val msgId = client.incr("id:messages:$chatId")
                    msgId match {
                        case Some(mid) =>
                            val time = System.currentTimeMillis
                            val msgData = Map("id" -> msgId, "ts" -> time, "sender" -> sender, "message" -> message)
                            // toJson
                            // pipe.zadd(s"messages:$chatId", mid, msgData.toJson)
                            true
                        case None =>
                            false
                    }
	            } finally {
	                Lock.release(s"chat:$chatId", lid)
	            }
	        case None =>
	            false
	    }
	}
}
