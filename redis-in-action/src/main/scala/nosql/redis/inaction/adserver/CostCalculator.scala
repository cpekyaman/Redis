package nosql.redis.inaction.adserver

sealed class AdType(val name: String)
case object CPM extends AdType("CPM")
case object CPA extends AdType("CPA")
case object CPC extends AdType("CPC")

object CostCalculator {
    // rvalue = TO_ECPM[type]( 1000, AVERAGE_PER_1K.get(type, 1), value)
    def calculate(adType: AdType, value: Double): Double = 
        adType match {
            case CPM => 1.0
            case CPC => 1.0
            case CPA => 1.0
            case _ => 1.0
        }
    
	private def cpc2ecpm(views: Int, clicks: Int, cpc: Double): Double = convert(views, clicks, cpc)
	
	private def cpa2ecpm(views: Int, actions: Int, cpa: Double): Double = convert(views, actions, cpa)
		
    private def convert(views: Int, events: Int, ratio: Double): Double = 
        return 1000.0 * ratio * events / views
}
