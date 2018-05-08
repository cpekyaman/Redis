package nosql.redis.inaction.support

import org.joda.time.DateTime
import com.redis.RedisCommand

object Utils {
    def rotateIfRequired(conn: RedisCommand, destination: String) {
        val startKey = s"${destination}:start"
        val now = DateTime.now
        val startHour = now.hourOfDay.get
        
        val existing = conn.get(startKey)        
        if(existing.exists{ _.toInt < startHour}) {
            conn.rename(destination, s"${destination}:last")
            conn.rename(startKey, s"${destination}:pstart")
            conn.set(startKey, startHour)
        }
    }
}
