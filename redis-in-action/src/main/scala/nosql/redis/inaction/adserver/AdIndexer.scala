package nosql.redis.inaction.adserver

import com.redis.RedisClient

import nosql.redis.inaction.search.Indexer

object AdIndexer {
    def index(conn: RedisClient, adId: String, locations: List[String], content: String, adType: AdType, adValue: Double) {
        conn.pipeline { pipe =>
            locations.foreach { loc => pipe.sadd(locIndex(loc), adId) }
            val words = Indexer.tokenize(content) 
            words.foreach { word => pipe.zadd(wordIndex(word), 1, adId) }
            
            pipe.hset("ads:type", adId, adType.name)
            pipe.zadd("ads:value", CostCalculator.calculate(adType, adValue), adId)
            pipe.zadd("ads:base_value", adValue, adId)
            
            pipe.sadd("ad:terms:" + adId, words.head, words.tail:_*)
        }
    }
    
    def locIndex(location: String): String = s"idx:loc:$location"
    
    def wordIndex(word: String): String = s"idx:adword:$word"
}
