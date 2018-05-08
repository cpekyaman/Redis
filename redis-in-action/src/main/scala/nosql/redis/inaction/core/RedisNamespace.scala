package nosql.redis.inaction.core

object RedisNamespace {	
	val articlesNs = "articles"
	val scoreNs = "articles:score"
	val timeNs = "articles:time"
	
	val articleNs = "article:"	
	val voteNs = "article:voted:"
	
	val groupNs = "group:"
	val loginNs = "login:"
	val userNs = "user:"
	val usersNs = "users"
	val recentNs = "recent:"
	val viewedNs = "viewed:"
	val itemNs = "item:"
	val cartNs = "cart:"
	val cacheNs = "cache:"
	val delayNs = "delay:"
	val scheduleNs = "schedule:"
	val inventoryNs = "inventory:"
	val userInventoryNs = "userInventory:"
	val marketNs = "market:"
	
	def articleWithNs(id: Long) = articleNs + id
	def voteWithNs(id: Long) = voteNs + id
	def groupWithNs(name: String) = groupNs + name
	def userWithNs(id: Long) = userNs + id	
	def itemWithNs(id: Long) = itemNs + id
	
	def viewedWithNs(token: String) = viewedNs + token
	def cart(session: String) = cartNs + session
	def cacheKey(requestId: String) = cacheNs + requestId
	def inventoryKey(row: String) = inventoryNs + row
	def userInventory(userId: Long) = userInventoryNs + userId
}
