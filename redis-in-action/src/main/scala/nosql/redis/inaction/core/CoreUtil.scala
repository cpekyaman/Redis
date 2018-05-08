package nosql.redis.inaction.core

import java.io.File
import scala.io.Source
import scala.util.matching.Regex

object CoreUtil {
    lazy val basePath = new File(".").getCanonicalPath + "\\src\\main\\resources\\"
    
    def getFullPath(fileName: String, path: Option[String] = None) = 
        CoreUtil.basePath + path.map(_ + "\\").getOrElse("") + fileName
}
