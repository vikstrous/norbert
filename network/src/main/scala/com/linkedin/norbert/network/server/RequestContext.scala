package com.linkedin.norbert.network.server

import scala.collection.mutable.Map

trait RequestContext {
  val attributes: Map[String, Any] = Map.empty[String, Any]
}

