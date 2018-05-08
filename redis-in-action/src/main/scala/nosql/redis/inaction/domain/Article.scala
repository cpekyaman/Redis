package nosql.redis.inaction.domain

import nosql.redis.inaction.core.{RedisNamespace,RedisService,FileUtil}
import RedisNamespace._
import com.redis.RedisClient
import RedisClient._

object Article {
    val alnumFields = Set("title","summary","link")
    
	val oneWeekSeconds = 7 * 84600
	val articlesPerPage = 10
	
	def vote(userId: Long, articleId: Long){
	    val conn = RedisService.getClient
	    val cutOff = System.currentTimeMillis - oneWeekSeconds
	    
	    val zscoreResult = conn.zscore(timeNs, articleWithNs(articleId))
	    zscoreResult match {
	        case Some(score) if(score >= cutOff) => 
	            // update which users voted for article
	            val result = conn.sadd(voteWithNs(articleId), userId)
	            
                result match {
                    case Some(d) if(d == 0) =>
                        // increment articles vote score in votes set
                        conn.zincrby(scoreNs, 1, articleId)
                        // update vote count in article hash
                        conn.hincrby(articleWithNs(articleId), "votes", 1)
                        
                    case None => println("could not add vote")
                }
	        case _ => println("No score returned")
	    }    
	}
	
	def post(conn: Option[RedisClient], userId: Long, title: String, link: String, summary: String): Option[Long] = {
	    val client = conn getOrElse RedisService.getClient
	    
	    val idResult = client.incr(articleNs)	    
	    idResult match {
	        case Some(id) =>
	            // set which users voted for article
	            val voted = voteWithNs(id)
                client.sadd(voted, userId)
                client.expire(voted, oneWeekSeconds)
                
                val now = System.currentTimeMillis
                val article = articleWithNs(id)
                client.pipeline{ pipe =>
                    pipe.hmset(article, Map(
                        "id" -> id,
                        "title" -> title,
                        "summary" -> summary,
                        "link" -> link,
                        "poster" -> userId,
                        "created" -> now,
                        "updated" -> now,
                        "votes" ->  1
                    ))
                    
                    // add article id to ids                    
                    pipe.sadd(articlesNs, id)
                    // add initial vote to scores set
                    pipe.zadd(scoreNs, 1, id)
                    // add create time to recent articles set
                    pipe.zadd(timeNs, now, id)
                }
                
                return Some(id)
                
	        case None => 
	            println("No id generated")
	            None
	    }
	} 
	
	def list(page: Integer, orderKey: String = scoreNs): Option[List[Map[String, String]]] = {
	    val conn = RedisService.getClient
	 
	    val start = (page-1) * articlesPerPage
        val end = start + articlesPerPage - 1
        
        list(conn, start, end, orderKey)
	}
	
	private def list(conn: RedisClient, start: Integer, end: Integer, orderKey: String): Option[List[Map[String, String]]] = {
	    val results = conn.zrange(orderKey, start, end, DESC)
        results match {
            case Some(list) =>
               var articles = List[Map[String, String]]()
               for(id <- list) {
                val article = conn.hgetall[String, String](id)
                article.toList ::: articles
               }               
               Some(articles)
               
            case None => println("No results")
               None 
        }
	}
	
	def addRemoveGroup(articleId: Long, groupsToAdd: List[String], groupsToRemove: List[String]) {
	    val conn = RedisService.getClient
	    for(gname <- groupsToAdd) {
	        conn.sadd(groupWithNs(gname), articleWithNs(articleId))
	    }
	    for(gname <- groupsToRemove) {
	        conn.srem(groupWithNs(gname), articleWithNs(articleId))
	    }
	}
	
	def listGroup(group: String, page: Integer, orderKey: String = "score:"): Option[List[Map[String, String]]] = {
	    val conn = RedisService.getClient
	
	    val key = orderKey + group
	    if(! conn.exists(key)) {
	        conn.zinterstore(key, List(groupWithNs(group), orderKey), MAX)
	        conn.expire(key, 60)
	    }
	
	    val start = (page-1) * articlesPerPage
        val end = start + articlesPerPage - 1
        
        list(conn, start, end, key)
	}
	
	def main(args: Array[String]) {
	    import nosql.redis.inaction.search.Indexer
	    
	    val conn = RedisService.getClient
	    val rand = new java.util.Random()
	    
	    val articles = FileUtil.readLines("article_list", Some("articles")).map(_.split("\\,").toList)   	    
	    articles.foreach { fu =>
	        val file = fu.head
	        val url = fu.tail.head	        
	        
	        val articleId = post(Some(conn), rand.nextInt(4), file, url, file)
	        articleId.map(_.toString).foreach{ id => Indexer.indexFile(Some(conn), "articles", file, Some(id)) }
	    }    
	}
}
