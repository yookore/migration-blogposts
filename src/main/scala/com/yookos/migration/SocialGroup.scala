package com.yookos.migration;

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.mapper._

case class SocialGroup(
    id: String, 
    groupid: Long, 
    grouptype: Int, 
    userid: String, 
    author: String, 
    jiveuserid: Long, 
    name: String,
    displayname: String, 
    description: String, 
    status: Long, 
    creationdate: Long, 
    modificationdate: Long) 
  extends Serializable

object SocialGroup {
  implicit object Mapper extends DefaultColumnMapper[SocialGroup](
    Map("id" -> "id",
      "groupid" -> "groupid",
      "grouptype" -> "grouptype",
      "userid" -> "userid",
      "author" -> "author",
      "jiveuserid" -> "jiveuserid",
      "name" -> "name",
      "displayname" -> "displayname",
      "description" -> "description",
      "status" -> "status",
      "creationdate" -> "creationdate",
      "modificationdate" -> "modificationdate"
      )  
  )

}
