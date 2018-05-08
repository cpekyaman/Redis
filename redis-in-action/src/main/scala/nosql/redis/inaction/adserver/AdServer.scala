package nosql.redis.inaction.adserver

import com.redis.{RedisClient,RedisCommand}
import RedisClient._
import nosql.redis.inaction.search.Indexer

object AdServer {
    def target(conn: RedisClient, locations: List[String], content: String): Option[Tuple2[String, Long]] = {
        var targetAd: Option[Tuple2[String, Double]] = None
        var targetId: Long = -1L
        
        conn.pipeline { pipe =>
            val (ads, adValues) = getAdsForLocations(pipe, locations)
            val (targetedAds, words) = finishScoring(pipe, ads, adValues, content)
            
            targetId = pipe.incr("ads:served").getOrElse(0L)                
            targetAd = pipe.zrangeWithScore[String](targetedAds, 0, 0, DESC).toList.flatMap( _.map{Some(_)} ).head
            
            targetAd match {
                case Some((adId, adScore)) if(targetId > 0) => AdRecorder.recordTargetingResult(pipe, targetId, adId, words)
            }
        }
        
        targetAd map { a => (a._1, targetId) }
    }
    
    private def getAdsForLocations(conn: RedisCommand, locations: List[String]): Tuple2[String, String] = {
        val setIds = locations.map { loc => s"idx:loc:$loc" }
        val matchedAdsSet = newTargetKey
        conn.sunionstore(matchedAdsSet, setIds:_*)
        
        val adsWithValueSet = newTargetKey  
        conn.zinterstoreWeighted(adsWithValueSet, List((matchedAdsSet, 0.0D), ("ads:value", 1.0D)), MAX)
        
        (matchedAdsSet, adsWithValueSet)
    }
    
    //TODO: No matching content
    private def finishScoring(conn: RedisCommand, adsMatched: String, adValues: String, content: String): Tuple2[String, List[String]] = {        
        val words = Indexer.tokenize(content)
        val wordBonuses = words.map { word => 
            val wordBonus = newBonusTarget(word)
            conn.zinterstoreWeighted(wordBonus, List((adsMatched, 0.0D), (AdIndexer.wordIndex(word), 1.0D)), SUM) 
            wordBonus
        }
        val bonusEcpm = wordBonuses.map{ wb => (wb, 1.0) }
        
        val min = newTargetKey
        conn.zunionstoreWeighted(min, bonusEcpm, MIN)
        
        val max = newTargetKey
        conn.zunionstoreWeighted(max, bonusEcpm, MAX)
        
        val result = newTargetKey
        conn.zunionstoreWeighted(result, List((adValues, 1.0), (min, 0.5), (max, 0.5)), SUM)
        
        (result, words)
    }   
    
    private def newTargetKey: String = "idx:ads:" + java.util.UUID.randomUUID().toString()
    
    private def newBonusTarget(word: String): String = s"idx:adBonus:$word"
    
    def main(args: Array[String]) {
        val result: Option[List[(String, Double)]] = Some(List(("cenk", 3.0), ("cengiz", 2.0)))
        result.foreach(println)
        result.toList.foreach(println)
        result.toList.head.foreach(println)
        result.toList.flatMap( _.map{a => Some(a)} ).foreach(println)
        println(result.toList.flatMap( _.map{a => Some(a)} ).head)
    }
}
