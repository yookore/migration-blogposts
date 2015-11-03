package com.yookos.migration

import akka.actor.{ Actor, Props, ActorSystem, ActorRef }
import akka.pattern.{ ask, pipe }
import akka.event.Logging
import akka.util.Timeout

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming.{ Milliseconds, Seconds, StreamingContext, Time }
import org.apache.spark.streaming.receiver._

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.mapper._
import com.datastax.spark.connector.cql.CassandraConnector

import org.json4s._
import org.json4s.JsonDSL._
//import org.json4s.native.JsonMethods._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import org.apache.commons.lang.StringEscapeUtils
import org.joda.time.DateTime

/**
 * @author ${user.name}
 */
object App extends App {
  
  // Configuration for a Spark application.
  // Used to set various Spark parameters as key-value pairs.
  val conf = new SparkConf(false) // skip loading external settings
  
  val mode = Config.mode
  Config.setSparkConf(mode, conf)
  val cache = Config.redisClient(mode)
  //val ssc = new StreamingContext(conf, Seconds(2))
  //val sc = ssc.sparkContext
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val system = SparkEnv.get.actorSystem
  
  createSchema(conf)
  
  implicit val formats = DefaultFormats

  // serializes objects from redis into
  // desired types
  //import com.redis.serialization._
  //import Parse.Implicits._
  
  val keyspace = Config.cassandraConfig(mode, Some("keyspace"))
  val totalLegacyUsers = 2124155L
  var cachedIndex = if (cache.get("latest_legacy_blogpost_index") == null) 0 else cache.get("latest_legacy_blogpost_index").toInt

  // Using the mappings table, get the profiles of
  // users from 192.168.10.225 and dump to mongo
  // at 10.10.10.216
  val mappingsDF = sqlContext.load("jdbc", Map(
    "url" -> Config.dataSourceUrl(mode, Some("mappings")),
    //"dbtable" -> "legacyusers")
    "dbtable" -> f"(SELECT userid, cast(yookoreid as text), username FROM legacyusers offset $cachedIndex%d) as legacyusers")
  )

  
  val blogsDF = sqlContext.load("jdbc", Map(
    "url" -> Config.dataSourceUrl(mode, Some("legacy")),
    "dbtable" -> "jiveblogpost")
  )
  
  val df = mappingsDF.select(mappingsDF("userid"), mappingsDF("yookoreid"), mappingsDF("username"))

  reduce(df)
  
  def reduce(df: DataFrame) = {
    df.collect().foreach(row => {
      cachedIndex = cachedIndex + 1
      cache.set("latest_legacy_blogpost_index", cachedIndex.toString)
      val userid = row.getLong(0)
      upsert(row, userid)
    })
  }

  def upsert(row: Row, userid: Long) = {
    blogsDF.select(
      blogsDF("blogpostid"), blogsDF("subject"), blogsDF("userid"), 
      blogsDF("permalink"), blogsDF("body"), blogsDF("publishdate"),
      blogsDF("creationdate"), blogsDF("modificationdate"))
        .filter(f"userid = $userid%d").foreach {
          blogItem => 
            val id = java.util.UUID.randomUUID()
            val blogpostid = Some(blogItem.getLong(0))
            val subject = blogItem.getString(1)
            val jiveuserid = Some(userid) // needed for legacy comments on status
            val yookoreid = row.getString(1)
            val author = row.getString(2) // same as username
            val permalink = Some(blogItem.getString(3))
            val body = blogItem.getString(4)
            val publishdate = Some(blogItem.getLong(5))
            val creationdate = blogItem.getLong(6)
            val modificationdate = blogItem.getLong(7)
            val commentCount = Some(0)
            val atmention = Some(null)
            val deleted = false
            val likeCount = Some(0)
            val location = Some(null)
            val tags = Some[Seq[String]](null)
            val uriImage = Some[Seq[java.util.UUID]](null)
            val url = Some(null)
            val urlthumbnail = Some(null)
            val viewCount = Some(0)

            sc.parallelize(Seq(BlogPost(
              id, blogpostid, subject, yookoreid, author, jiveuserid, 
              permalink, body, publishdate, creationdate, modificationdate,
              commentCount, atmention, deleted, likeCount, location,
              tags, uriImage, url, urlthumbnail, viewCount)))
                .saveToCassandra(s"$keyspace", "legacyblogposts", 
                  SomeColumns("id", "blogpostid", "title", "userid",
                    "author", "jiveuserid", "permalink", "body",
                    "publishdate", "created_at", "updated_at",
                    "comment_count", "at_mention", "deleted", "like_count",
                    "location", "tags", "uri_image", "url", "url_thumbnail",
                    "view_count"
                  )
                )
        }
        println("===Latest group cachedIndex=== " + cache.get("latest_legacy_blogpost_index").toInt)
    
  }
  
  blogsDF.printSchema()

  def createSchema(conf: SparkConf): Boolean = {
    val keyspace = Config.cassandraConfig(mode, Some("keyspace"))
    val replicationStrategy = Config.cassandraConfig(mode, Some("replStrategy"))
    CassandraConnector(conf).withSessionDo { sess =>
      sess.execute(s"DROP TABLE IF EXISTS $keyspace.legacyblogposts")
      sess.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = $replicationStrategy")
      sess.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.legacyblogposts ( " +
        "id timeuuid, " + 
        "blogpostid bigint, " +
        "userid text, " + 
        "author text, " + 
        "jiveuserid bigint, " + 
        "title text, " + 
        "permalink text, " + 
        "body text, " + 
        "created_at timestamp, " + 
        "updated_at timestamp, " + 
        "publishdate bigint, " +
        "comment_count int, " +
        "at_mention list<text>, " +
        "deleted boolean, " +
        "like_count int, " +
        "location text, " +
        "tags list<text>, " +
        "uri_image list<uuid>, " +
        "url text, " +
        "url_thumbnail text, " +
        "view_count int, " +
        "PRIMARY KEY ((id, userid), author) ) " + 
      "WITH CLUSTERING ORDER BY (author DESC)")
    } wasApplied
  }
}
