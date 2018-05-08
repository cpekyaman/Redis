package nosql.redis.inaction.support

import scala.collection.mutable.ListBuffer
import nosql.redis.inaction.core.{RedisService,RedisNamespace}

object Counters {    
    private val precisions = List(1, 5, 60, 300, 3600, 18000, 86400)
    private val sampleCount = 120
    
    def update(name: String, count: Int = 1) {
        val conn = RedisService.getClient        
        val now = System.currentTimeMillis
        
        val allCommands = new ListBuffer
        for(prec <- precisions) {
            val pnow = (now / prec).toInt * prec
            val counterKey = counter(name, prec)            
            allCommands ++ List(
                {() => conn.zadd("known:", 0, counterKey) },
                {() => conn.hincrby(counterKey, pnow, count) }
            )
        }
        
        conn.pipelineNoMulti(allCommands)
    }
    
    def get(name: String, precision: Int): List[(Long, Long)] = {
        import com.redis.serialization.Parse.Implicits.parseLong
        
        val conn = RedisService.getClient
        val counterKey = counter(name, precision)
        val values = conn.hgetall[Long, Long](counterKey)
        values.getOrElse(Map[Long,Long]()).toList.sortWith { (a,b) => a._1 < b._1 }
    }
    
    def cleanup() {
        val conn = RedisService.getClient
        val start = System.currentTimeMillis
        
        var passes = 0
        var index = 0
        while(conn.zcard("known:").exists( _ > index)) {
            val counterKey = conn.zrange("known:", index, index)
            index += 1    
            counterKey match {
                case Some(counter :: _) =>
                    val precision = counter.split(':')(1).toInt
                    val loopPrecision = precision
                    if(passes % loopPrecision > 0) {
                        import com.redis.serialization.Parse.Implicits.parseLong
                        
                        val cutoff = start - (sampleCount * precision)
                        val samples = conn.hkeys[Long](counter).getOrElse(List()).sorted
                        val toRemove = samples.filter{ _ < cutoff}
                        if(! toRemove.isEmpty) {
                            conn.hdel(counter, toRemove.head, toRemove.tail:_*)
                            //TODO: need a watch for counter (hlen & zrem)                             
                            if(toRemove.size == samples.size) {
                                if(conn.hlen(counter).exists{ _ == 0}) {                                    
                                    conn.zrem("known:", counter)
                                    index -= 1
                                }
                            }
                        }
                    }
                case _ =>
                    println("")
            }
        }
        passes += 1
        //TODO: add a sleep
        /*
        duration = min(int(time.time() - start) + 1, 60) 
        time.sleep(max(60 - duration, 1))
        */
        
    }
    
    private def counter(name: String, prec: Int) = s"counter:${prec}:${name}"
}
