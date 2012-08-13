/*
 * Copyright 2009-2010 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.norbert
package network


import server.{RequestContext, NetworkServer}
import netty.{RequestContext => NettyRequestContext, NetworkServerConfig, NettyServerFilter}
import org.jboss.netty.logging.{InternalLoggerFactory, Log4JLoggerFactory}
import com.google.protobuf.Message
import protos.NorbertExampleProtos
import cluster.ClusterClient
import norbertutils._
import network.NorbertNetworkServerMain.LogFilter
import protos.NorbertProtos.NorbertMessage


object NorbertNetworkServerMain {
  InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory)

  def main(args: Array[String]) {
    val cc = ClusterClient(args(0), args(1), 30000)
    cc.awaitConnectionUninterruptibly
    cc.removeNode(1)
    cc.addNode(1, "localhost:31313", Set())

    val config = new NetworkServerConfig
    config.clusterClient = cc

    val ns = NetworkServer(config)

    ns.registerHandler(pingHandler)
    ns.addFilters(List(new LogFilter))

    ns.bind(args(2).toInt)

    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run = {
        cc.shutdown
      }
    })
  }

  private def pingHandler(ping: Ping): Pong = {
    println("Requested ping from client %d milliseconds ago (assuming synchronized clocks)".format(ping.timestamp - System.currentTimeMillis) )
    Pong(System.currentTimeMillis)
  }

  class LogFilter extends NettyServerFilter  {
    val clock = SystemClock
    def onRequest(request: Any, context: RequestContext)
    { context.attributes += ("START_TIMER" -> clock.getCurrentTime) }

    def onResponse(response: Any, context: RequestContext)
    { val start: Long = context.attributes.getOrElse("START_TIMER", -1).asInstanceOf[Long]
      println("server side time logging: " + (clock.getCurrentTime - start) + " ms.")
    }

    def onMessage(message: NorbertMessage, context: RequestContext) =
    { context.attributes += ("PRE_SERIALIZATION" -> clock.getCurrentTime) }

    def postMessage(message: NorbertMessage, context: RequestContext) =
    {
      val start: Long = context.attributes.getOrElse("PRE_SERIALIZATION", -1).asInstanceOf[Long]
      println("server side time logging including serialization: " + (clock.getCurrentTime - start) + " ms.")
    }

    def onError(error: Exception, context: RequestContext)
    {}
  }
}
