package nosql.redis.inaction.search

import nosql.redis.inaction.core.FileUtil
import java.io.File
import scala.io.Source
import scala.util.matching.Regex

object SearchUtil {  
    lazy val stopWords = FileUtil.toString("stop_words.txt").split(" ").toSet
    
    def regexFind(regex: Regex, content: String): List[String] = 
        regex.findAllIn(content).filterNot( stopWords.contains(_) ).toList
}
