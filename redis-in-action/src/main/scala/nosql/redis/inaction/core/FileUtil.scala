package nosql.redis.inaction.core

import scala.io.Source

object FileUtil {
	def toString(fileName: String, path: Option[String] = None): String 
	    = Source.fromFile(CoreUtil.getFullPath(fileName, path)).mkString
	
	def readLines(fileName: String, path: Option[String] = None): Iterator[String] 
	    = Source.fromFile(CoreUtil.getFullPath(fileName, path)).getLines
}
