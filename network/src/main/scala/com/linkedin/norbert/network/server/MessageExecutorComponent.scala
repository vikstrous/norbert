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
package server

import logging.Logging
import jmx.JMX.MBean
import jmx.{FinishedRequestTimeTracker, JMX}
import actors.DaemonActor
import java.util.concurrent.atomic.AtomicInteger
import norbertutils.{SystemClock, NamedPoolThreadFactory}
import java.util.concurrent._
import scala.collection.mutable.MutableList
import common.CachedNetworkStatistics
import util.ProtoUtils
import norbertutils._
import cluster.{ClusterDisconnectedException, ClusterClientComponent}

/**
 * A component which submits incoming messages to their associated message handler.
 */
trait MessageExecutorComponent {
  val messageExecutor: MessageExecutor
}

trait MessageExecutor {
  def executeMessage[RequestMsg, ResponseMsg](request: RequestMsg, responseHandler: (Either[Exception, ResponseMsg]) => Unit)
                                             (implicit is: RequestInputSerializer[RequestMsg]) : Unit = executeMessage(request, responseHandler, None)
  def executeMessage[RequestMsg, ResponseMsg](request: RequestMsg, responseHandler: (Either[Exception, ResponseMsg]) => Unit, context: Option[RequestContext])
                                             (implicit is: RequestInputSerializer[RequestMsg]): Unit
  @volatile val filters : MutableList[Filter]
  def addFilters(filters: List[Filter]) : Unit = this.filters ++= (filters)
  def shutdown: Unit
}

class ThreadPoolMessageExecutor(clientName: Option[String],
                                serviceName: String,
                                messageHandlerRegistry: MessageHandlerRegistry,
                                val filters: MutableList[Filter],
                                requestTimeout: Long,
                                corePoolSize: Int,
                                maxPoolSize: Int,
                                keepAliveTime: Int,
                                maxWaitingQueueSize: Int,
                                requestStatisticsWindow: Long) extends MessageExecutor with Logging {
  def this(clientName: Option[String],
           serviceName: String,
           messageHandlerRegistry: MessageHandlerRegistry,
           requestTimeout: Long,
           corePoolSize: Int,
           maxPoolSize: Int,
           keepAliveTime: Int,
           maxWaitingQueueSize: Int,
           requestStatisticsWindow: Long) =
    this(clientName, serviceName, messageHandlerRegistry, new MutableList[Filter], requestTimeout, corePoolSize, maxPoolSize, keepAliveTime, maxWaitingQueueSize, requestStatisticsWindow)

  private val statsActor = CachedNetworkStatistics[Int, Int](SystemClock, requestStatisticsWindow, 200L)
  private val totalNumRejected = new AtomicInteger

  val requestQueue = new ArrayBlockingQueue[Runnable](maxWaitingQueueSize)
  val statsJmx = JMX.register(new RequestProcessorMBeanImpl(clientName, serviceName, statsActor, requestQueue))

  private val threadPool = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS, requestQueue,
    new NamedPoolThreadFactory("norbert-message-executor")) {

    override def beforeExecute(t: Thread, r: Runnable) = {
      val rr = r.asInstanceOf[RequestRunner[_, _]]

      statsActor.beginRequest(0, rr.id)
    }

    override def afterExecute(r: Runnable, t: Throwable) = {
      val rr = r.asInstanceOf[RequestRunner[_, _]]
      statsActor.endRequest(0, rr.id)
    }
  }

  def executeMessage[RequestMsg, ResponseMsg](request: RequestMsg, responseHandler: (Either[Exception, ResponseMsg]) => Unit, context: Option[RequestContext] = None)
                                             (implicit is: RequestInputSerializer[RequestMsg]) {
    val rr = new RequestRunner(request, context, filters, responseHandler, is = is)
    try {
      threadPool.execute(rr)
    } catch {
      case ex: RejectedExecutionException =>
        statsActor.endRequest(0, rr.id)

        totalNumRejected.incrementAndGet
        log.warn("Request processing queue full. Size is currently " + requestQueue.size)
        throw new HeavyLoadException
    }
  }

  def shutdown {
    threadPool.shutdown
    statsJmx.foreach { JMX.unregister(_) }
    log.debug("MessageExecutor shut down")
  }

  private val idGenerator = new AtomicInteger(0)

  private class RequestRunner[RequestMsg, ResponseMsg](request: RequestMsg,
                                                       context: Option[RequestContext],
                                                       filters: MutableList[Filter],
                                                       callback: (Either[Exception, ResponseMsg]) => Unit,
                                                       val queuedAt: Long = System.currentTimeMillis,
                                                       val id: Int = idGenerator.getAndIncrement.abs,
                                                       implicit val is: RequestInputSerializer[RequestMsg]) extends Runnable {
    def run = {
      val now = System.currentTimeMillis

      if(now - queuedAt > requestTimeout) {
        totalNumRejected.incrementAndGet
        log.warn("Request timed out, ignoring! Currently = " + now + ". Queued at = " + queuedAt + ". Timeout = " + requestTimeout)
        if(callback != null){
          callback(Left(new HeavyLoadException))
        }
      } else {
        log.debug("Executing message: %s".format(request))

        val response: Option[Either[Exception, ResponseMsg]] =
        try {
          filters.foreach(filter => continueOnError(filter.onRequest(request, context.getOrElse(null))))
          val handler = messageHandlerRegistry.handlerFor[RequestMsg, ResponseMsg](request)
          try {
            val response = handler(request)
            response match {
              case null => None
              case _:Unit => None
              case _ => Some(Right(response))
            }
          } catch {
            case ex: Exception =>
              log.error(ex, "Message handler threw an exception while processing message")
              Some(Left(ex))
          }
        } catch {
          case ex: InvalidMessageException =>
            log.error(ex, "Received an invalid message: %s".format(request))
            Some(Left(ex))

          case ex: Exception =>
            log.error(ex, "Unexpected error while handling message: %s".format(request))
            Some(Left(ex))
        }
        response.foreach { (res) =>
          if(callback!= null){
            callback(res)
          }
          res match {
            case Left(ex) => filters.reverse.foreach(filter => continueOnError(filter.onError(ex, context.getOrElse(null))))
            case Right(responseMsg) =>  filters.reverse.foreach(filter => continueOnError(filter.onResponse(responseMsg, context.getOrElse(null))))
          }
        }
      }
    }
  }

  trait RequestProcessorMBean {
    def getQueueSize: Int

    def getTotalNumRejected: Int

    def getMedianTime: Double
  }

  class RequestProcessorMBeanImpl(clientName: Option[String], serviceName: String, val stats: CachedNetworkStatistics[Int, Int], queue: ArrayBlockingQueue[Runnable])
    extends MBean(classOf[RequestProcessorMBean], JMX.name(clientName, serviceName)) with RequestProcessorMBean {
    def getQueueSize = queue.size

    def getTotalNumRejected = totalNumRejected.get.abs

    def getMedianTime = stats.getStatistics(0.5).map(_.finished.values.map(_.percentile)).flatten.sum
  }
}

