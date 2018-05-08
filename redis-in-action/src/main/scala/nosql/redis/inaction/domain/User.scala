package nosql.redis.inaction.domain

import nosql.redis.inaction.core.{RedisNamespace,RedisService}
import RedisNamespace._

class User(val id: Long, val name: String, val lastName: String, val email: String, val password: String)

object User {
    def apply(name: String, lastName: String, email: String, password: String) = new User(0L,name,lastName,email,password)
    
    def create(name: String, lastName: String, email: String, password: String): Option[Long] = {
        create(User(name,lastName,email,password))
    }
    
    def create(user: User): Option[Long] = {
        val conn = RedisService.getClient
        val idResult = conn.incr(userNs)
        
        idResult match {
            case Some(id) =>
                val now = System.currentTimeMillis
                val userKey = userWithNs(id)
                conn.pipeline{pipe =>
                    pipe.hmset(userKey, Map(
                        "id" -> id,
                        "name" -> user.name,
                        "last_name" -> user.lastName,
                        "email" -> user.email,
                        "password" -> user.password,
                        "created" -> now,
                        "updated" -> now
                    ))
                    pipe.sadd(usersNs, id)
                }
                
                Some(id)
            case None =>
                None
        }
    }
    
    def main(args: Array[String]) {
        User.create("cenk", "pekyaman", "cenkpekyaman@yahoo.com", "cenk123")
        User.create("cengiz", "pekyaman", "cengo@yahoo.com", "cengo123")
        User.create("ali", "veli", "aliveli@yahoo.com", "ali123")       
        for( i <- 1 to 5) {
            User.create(s"name-$i", s"lastname-$i", "user_$i@yahoo.com", "passwd_$i")
        }
    }
}
