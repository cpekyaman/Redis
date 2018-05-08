package nosql.redis.inaction.support

import nosql.redis.inaction.core.{RedisService,RedisNamespace}
import com.redis.RedisClient
import org.joda.time.DateTime
import com.github.nscala_time.time.Imports._

object LogLevel extends Enumeration {
  type LogLevel = Value

  final val DEBUG = Value("DEBUG")
  final val INFO = Value("INFO")
  final val ERROR = Value("ERROR")
}

object Logger {
    def addRecent(conn: Option[RedisClient], logName: String, message: String, logLevel: LogLevel.Value = LogLevel.INFO) {
        val client = conn getOrElse RedisService.getClient
        val now = System.currentTimeMillis
        
        val destination = s"recentLogs:${logName}:${logLevel.toString}"        
        val messageWithTime = s"${now} : ${message}"
        client.pipelineNoMulti(List(
            {() => client.lpush(destination, messageWithTime) },
            {() => client.ltrim(destination, 0, 99) }
        ))
    }
    
    def addCommon(logName: String, message: String, logLevel: LogLevel.Value = LogLevel.INFO) {
        val conn = RedisService.getClient        
        val destination = s"commonLogs:${logName}:${logLevel.toString}"
               
        //TODO: add exception handling & retry
        conn.pipeline { pipe =>
            //pipe.watch(startKey)            
            Utils.rotateIfRequired(pipe, destination)
            pipe.zincrby(destination, 1, message)
            //TODO: pipe does not extend from client, not in same transaction ??
            addRecent(Some(conn), logName, message, logLevel)
        }
    }    
}
