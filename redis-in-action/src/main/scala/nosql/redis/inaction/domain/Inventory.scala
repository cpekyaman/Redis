package nosql.redis.inaction.domain

object Inventory {
	def get(row: String): Any = row
	
	def toJson(rowData: Any): String = rowData.toString
}
