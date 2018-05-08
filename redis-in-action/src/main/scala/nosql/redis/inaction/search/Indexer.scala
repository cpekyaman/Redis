package nosql.redis.inaction.search

import nosql.redis.inaction.core.{RedisService,FileUtil}
import java.io.File
import scala.io.Source

import com.redis.RedisClient

object Indexer {   
    val Words = "\\b([A-Za-z\\-\\']){2,}\\b".r   
    
    def indexFile(conn: Option[RedisClient], path: String, fileName: String, id: Option[String]) {        
        indexContent(conn, id.getOrElse(fileName), fileContent(path, fileName))
    }
    
    def indexContent(conn: Option[RedisClient], name: String, content: String) {
        val client = conn getOrElse RedisService.getClient
        client.pipeline { pipe =>
            for(word <- tokenize(content)) {
                pipe.sadd(wordIndex(word), name)
            }
        }
    }
    
    def wordIndex(word: String): String = s"idx:words:$word"
    
    def tokenize(content: String): List[String] = SearchUtil.regexFind(Words, content)
        
    def fileContent(path: String, fileName: String) = FileUtil.toString(fileName, Some(path))
    
    def main(args: Array[String]) {
       
    }
}
