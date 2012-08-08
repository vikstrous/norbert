package com.linkedin.norbert
package network
package netty
/**
 * @auther: rwang
 * @date: 7/27/12
 * @version: $Revision$
 */

import server.{Filter => SFilter, RequestContext => SRequestContext}
import protos.NorbertProtos.NorbertMessage

trait Filter extends SFilter {
  def onMessage(message: NorbertMessage, context: SRequestContext) : Unit
  def postMessage(message: NorbertMessage, context: SRequestContext) : Unit
}

