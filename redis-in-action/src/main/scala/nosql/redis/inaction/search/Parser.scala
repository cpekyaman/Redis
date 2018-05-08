package nosql.redis.inaction.search

import scala.collection.mutable.{Set, ListBuffer}

object Parser {
    case class Query(all: List[Set[String]], unwanted: Set[String])
        
    val QueryTokens = "[\\+\\-]?([A-Za-z\\-\\']{2,})".r 
    
    def parse(query: String): Query = {
        val unwanted = Set[String]()
        val all = ListBuffer[Set[String]]()
        var current = Set[String]()
        
        for(token <- SearchUtil.regexFind(QueryTokens, query)) {
            var prefix: Option[Char] = None
            var word = token
            if(token.startsWith("+") || token.startsWith("-")) {
                prefix = Some(token.charAt(0))
                word = token.substring(1, token.length)                
            }
            
            prefix match {
                case Some('-') => unwanted += word
                case Some('+') => current += word
                case _ => 
                    if(! current.isEmpty) {
                        all += current
                        current = Set[String]()
                    }
                    current += word                                        
            }
        }
        if(! current.isEmpty) all += current 
        
        Query(all.toList, unwanted)
    }
}
