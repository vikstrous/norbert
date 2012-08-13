package com.linkedin.norbert.network.server

trait Filter {
  def onRequest(request: Any, context: RequestContext): Unit
  def onResponse(response: Any, context: RequestContext): Unit
  def onError(error: Exception, context: RequestContext): Unit
}