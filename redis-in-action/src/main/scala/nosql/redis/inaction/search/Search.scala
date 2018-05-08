package nosql.redis.inaction.search

import nosql.redis.inaction.domain.Article
import nosql.redis.inaction.core.{RedisService,RedisNamespace}
import RedisNamespace._

import scala.collection.mutable.{Set, ListBuffer}

import com.redis.RedisClient

sealed trait SetOperation
case object Union extends SetOperation
case object Intersect extends SetOperation
case object Diff extends SetOperation

case class SortOption(val field: String, val desc: Boolean)

object Search {
    import com.redis.serialization.Parse.Implicits.parseLong
    import Parser.Query
    
    def searchNSort(queryStr: String, 
                    sortBy: SortOption, 
                    start: Int = 0, 
                    limit: Int = 10, 
                    ttl: Int = 30): List[Long] = {
                    
        val result = search(queryStr, ttl)
        result match {
            case Some(id) => sort(id, sortBy, start, limit, ttl)
            case _ => Nil
        }
    }
    
    def sort(key: String, 
             sortBy: SortOption, 
             start: Int = 0,
             limit: Int = 10, 
             ttl: Int = 30): List[Long] = {
             
         val by = "${articleNs}*->${sortBy.field}"
         val alpha = Article.alnumFields.contains(sortBy.field)
            
         val conn = RedisService.getClient
         var results: List[Long] = Nil
         conn.pipeline { pipe => 
            if(pipe.expire(key, ttl)) {
                val memCount = pipe.scard(key)
                val sortResults: Option[List[Option[Long]]] = 
                    pipe.sort[Long](key, Some(Pair(start, limit)), sortBy.desc, alpha, Some(by))
                results = sortResults.toList.flatten.flatMap(_.toList)
            }            
         }                                               
         
         results
    }
    
    def search(queryStr: String, ttl: Int = 30): Option[String] = {
        val conn = RedisService.getClient
        val query = parse(queryStr)        
        
        query match {
             case Query(all, unwanted) if(all.size > 0) =>
                val toIntersect = processWords(conn, all)                
                val intersectResult: String = intersectResults(conn, toIntersect)
                    
                 if(! unwanted.isEmpty && ! intersectResult.isEmpty) 
                    Some(processExcludes(conn, intersectResult, unwanted))
                 else
                    Some(intersectResult)
                    
             case _ => None
        }
    }
    
    private def processWords(conn: RedisClient, words: Seq[Set[String]]): Seq[String] = {
        val toIntersect = ListBuffer[String]()
        for(ws <- words) {
            if(ws.size > 1) // union synonyms of a word
               toIntersect += find(conn, ws.toList, Union)
            else // index for single word
               toIntersect += Indexer.wordIndex(ws.head)              
        }
        
        toIntersect
    }
    
    private def intersectResults(conn: RedisClient, toIntersect: Seq[String]): String = 
        if(toIntersect.size > 1) // store intermediate results in a single set
            setOp(conn, Intersect, newTargetKey, toIntersect.toList)
        else
            toIntersect.head
    
    private def processExcludes(conn: RedisClient, 
                                intersectResult: String, 
                                unwanted: Set[String], 
                                ttl: Int = 30): String = 
        setOp(conn, Diff, newTargetKey, List(intersectResult, findExcludes(conn, unwanted, ttl)))
        
    private def findExcludes(conn: RedisClient, unwanted: Set[String], ttl: Int = 30): String = 
        if(unwanted.size > 1) 
            find(conn, unwanted.toList, Union, ttl)
        else
            Indexer.wordIndex(unwanted.head)
        
    def parse(query: String): Query = Parser.parse(query)
    
    def find(client: RedisClient, words: List[String], operation: SetOperation, ttl: Int = 30): String =  
        setOp(client, operation, newTargetKey, words.map(a => Indexer.wordIndex(a) ))

    private def newTargetKey: String = "search:" + java.util.UUID.randomUUID().toString()
    
    private def setOp(client: RedisClient, 
                      operation: SetOperation, 
                      target: String, 
                      sources: List[String], 
                      ttl: Int = 30): String = {
                      
        client.pipeline { pipe => 
            pipe.expire(target, ttl)
            operation match {
                case Union => 
                    // finds all documents having at least on of the words
                    pipe.sunionstore(target, sources:_*)
                case Intersect => 
                    // finds all documents having all the words
                    pipe.sinterstore(target, sources:_*)
                case Diff => 
                    // finds all documents having only one of the words
                    pipe.sdiffstore(target, sources:_*)
            }
        }
        
        println(s"\nstoring ${operation} of ${sources} in ${target}")
        target
    }
    
     def main(args: Array[String]) {
        val queryStr = """
                |array
                |string +String +strings +Strings
                |-double
            """.stripMargin
       
        val resultKey = search(queryStr).getOrElse("")       
        val searchResults = RedisService.getClient.smembers[String](resultKey).toList.flatten.flatMap(_.toList)
        
        println(s"\n${searchResults}\n")
     }
}
