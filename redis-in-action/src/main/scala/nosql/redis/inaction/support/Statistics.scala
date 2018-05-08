package nosql.redis.inaction.support

import nosql.redis.inaction.core.{RedisService,RedisNamespace}

object Statistics {
    import com.redis.RedisClient._
    
    def update(context: String, name: String, value: Int) {
        val conn = RedisService.getClient       
        val destination = s"statistics:${context}:${name}"
        
        //TODO: add exception handling & retry        
        conn.pipeline { pipe =>
            Utils.rotateIfRequired(pipe, destination)
            val tempKey1 = java.util.UUID.randomUUID().toString()
            val tempKey2 = java.util.UUID.randomUUID().toString()
            pipe.zadd(tempKey1, value, "min") 
            pipe.zadd(tempKey2, value, "max")
            
            pipe.zunionstore(destination, List(destination, tempKey1), MIN)
            pipe.zunionstore(destination, List(destination, tempKey2), MAX)
            
            pipe.del(tempKey1, tempKey2)
            pipe.zincrby(destination, 1, "count")
            pipe.zincrby(destination, value, "sum")
            pipe.zincrby(destination, value * value, "sumsq")
        }
    }
    
    def get(context: String, name: String): Option[Map[String, Double]] = {
        val conn = RedisService.getClient       
        val statKey = s"statistics:${context}:${name}"
        val statValues = conn.zrangeWithScore(statKey, 0, 1)
        statValues match {
            case Some(list) =>                
                val data = Map(list:_*)
                val avg = data.get("sum").getOrElse(0D) / data.get("count").getOrElse(1D)
                //TODO: calculate correct values
                val numerator = 1.0D //result.get("sumsq") - result.get("sum") ** 2 / result.get("count")
                val stddev = 1.0D//(numerator / (data['count']-1or1))**.5
                return Some(data ++ List(("average", avg), ("stddev", stddev)))                
            case None => 
                return None
        }
        None
    }
}
