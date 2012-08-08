package com.linkedin.norbert.network.server

import scala.collection.mutable.Map

/**
 * @auther: rwang
 * @date: 7/26/12
 * @version: $Revision$
 */
trait RequestContext
{
  val attributes : Map[String,  Any] = Map.empty[String, Any]
}

