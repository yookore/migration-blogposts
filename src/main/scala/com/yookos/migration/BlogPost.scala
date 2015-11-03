package com.yookos.migration;

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.mapper._

case class BlogPost(
    id: java.util.UUID, 
    blogpostid: Option[Long], 
    subject: String, 
    userid: String, 
    author: String, 
    jiveuserid: Option[Long], 
    permalink: Option[String],
    body: String, 
    publishdate: Option[Long], 
    creationdate: Long, 
    modificationdate: Long,
    commentcount: Option[Int],
    atmention: Option[String],
    deleted: Boolean,
    likecount: Option[Int],
    location: Option[String],
    tags: Option[Seq[String]],
    imageuri: Option[Seq[java.util.UUID]],
    url: Option[String],
    urlthumbnail: Option[String],
    viewcount: Option[Int]
    ) 
  extends Serializable

object BlogPost {
  implicit object Mapper extends DefaultColumnMapper[BlogPost](
    Map("id" -> "id",
      "blogpostid" -> "blogpostid",
      "subject" -> "title",
      "userid" -> "userid",
      "author" -> "author",
      "jiveuserid" -> "jiveuserid",
      "permalink" -> "permalink",
      "body" -> "body",
      "publishdate" -> "publishdate",
      "creationdate" -> "created_at",
      "modificationdate" -> "updated_at",
      "commentcount" -> "comment_count",
      "atmention" -> "at_mention",
      "deleted" -> "deleted",
      "likecount" -> "like_count",
      "location" -> "location",
      "tags" -> "tags",
      "imageuri" -> "uri_image",
      "url" -> "url",
      "urlthumbnail" -> "url_thumbnail",
      "viewcount" -> "view_count"
      )  
  )
}
