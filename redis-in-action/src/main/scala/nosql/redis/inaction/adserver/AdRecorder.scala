package nosql.redis.inaction.adserver

import com.redis.RedisCommand

object AdRecorder {
    def recordTargetingResult(conn: RedisCommand, targetId: Long, targetAd: String, words: List[String]) {
        val adTerms = conn.smembers(s"ad:terms:$targetAd").toList.flatMap(_.map{_.getOrElse("")}).filterNot(_.isEmpty)
        val matched = words.filter(adTerms.contains(_))
        if(! matched.isEmpty) {
            val matchedKey = getMatchedKey(targetId) 
            conn.sadd(matchedKey, matched.head, matched.tail:_*)
            conn.expire(matchedKey, 900)
        }
        
        conn.hget("ads:type", targetAd) match {
            case Some(adType) => conn.incr(s"ad:type:$adType:views")
            case None => // we do nothing here
        }
        
        matched.foreach { word => conn.zincrby("ad:views:$targetAd", 1, word) }
        
        conn.zincrby(s"ad:views:$targetAd", 1, targetAd) match {
            case Some(views) if((views % 100) == 0) => updateCPMs(conn, targetAd)
        }
    }
    
    private def getMatchedKey(targetId: Long): String = s"ad:terms:matched:$targetId"
    
    //TODO: implement
    private def updateCPMs(conn: RedisCommand, targetAd: String) {
    }
    
    //TODO: complete
    private def recordAdClick(conn: RedisCommand, targetId: Long, targetAd: String, recordAction: Boolean = true) {
        val recordKey = if(recordAction) s"ad:actions:$targetAd" else s"ad:clicks:$targetAd"
        val matchedKey = getMatchedKey(targetId)
        
        conn.hget("ads:type", targetAd) match {
            case Some(adType) if(adType.equals(CPA.name)) =>
                conn.expire(matchedKey, 900)
                if(recordAction) {
                    conn.incr(s"ads:type:$adType:actions")
                }   
            case Some(adType) => conn.incr(s"ads:type:$adType:clicks")
            case None => // do nothing
        }
        
        val matched = conn.smembers(matchedKey).toList.flatMap(_.map{_.getOrElse("")}).filterNot(_.isEmpty)
        matched.foreach { word => conn.zincrby(recordKey, 1, word) }
        conn.zincrby(recordKey, 1, targetAd)
        
        updateCPMs(conn, targetAd)
    }
}
