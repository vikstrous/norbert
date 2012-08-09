package com.linkedin.norbert
package network
package netty

import server.{Filter, RequestContext => SRequestContext}
import protos.NorbertProtos.NorbertMessage

trait NettyServerFilter extends Filter {
  def onMessage(message: NorbertMessage, context: SRequestContext) : Unit
  def postMessage(message: NorbertMessage, context: SRequestContext) : Unit
}

