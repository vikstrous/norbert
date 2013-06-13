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

trait MessageHandlerRegistryComponent {
  val messageHandlerRegistry: MessageHandlerRegistry
}

private case class MessageHandlerEntry[RequestMsg, ResponseMsg]
(is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg], handler: RequestMsg => ResponseMsg)
private case class SimpleMessageHandlerEntry[RequestMsg]
(is: RequestInputSerializer[RequestMsg], handler: RequestMsg => Unit)

class MessageHandlerRegistry {
  @volatile private var handlerMap =
    Map.empty[String, MessageHandlerEntry[_ <: Any, _ <: Any]]
  @volatile private var simpleHandlerMap =
    Map.empty[String, SimpleMessageHandlerEntry[_ <: Any]]

  def registerHandler[RequestMsg, ResponseMsg](handler: RequestMsg => ResponseMsg)
                                              (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) {
    if(handler == null) throw new NullPointerException

    handlerMap += (is.requestName -> MessageHandlerEntry(is, os, handler))
  }

  def registerSimpleHandler[RequestMsg](handler: RequestMsg => Unit)
                                 (implicit is: RequestInputSerializer[RequestMsg]) {
    if(handler == null) throw new NullPointerException

    simpleHandlerMap += (is.requestName -> SimpleMessageHandlerEntry(is, handler))
  }

  @throws(classOf[InvalidMessageException])
  def simpleInputSerializerFor[RequestMsg](messageName: String): RequestInputSerializer[RequestMsg] = {
    simpleHandlerMap.get(messageName).map(_.is)
      .getOrElse(throw buildException(messageName))
      .asInstanceOf[RequestInputSerializer[RequestMsg]]
  }

  @throws(classOf[InvalidMessageException])
  def inputSerializerFor[RequestMsg, ResponseMsg](messageName: String): InputSerializer[RequestMsg, ResponseMsg] = {
    handlerMap.get(messageName).map(_.is)
      .getOrElse(throw buildException(messageName))
      .asInstanceOf[InputSerializer[RequestMsg, ResponseMsg]]
  }

  @throws(classOf[InvalidMessageException])
  def outputSerializerFor[RequestMsg, ResponseMsg](messageName: String): OutputSerializer[RequestMsg, ResponseMsg] = {
    handlerMap.get(messageName).map(_.os)
      .getOrElse(throw buildException(messageName))
      .asInstanceOf[OutputSerializer[RequestMsg, ResponseMsg]]
  }

  @throws(classOf[InvalidMessageException])
  def handlerFor[RequestMsg, ResponseMsg](request: RequestMsg)
                                         (implicit is: RequestInputSerializer[RequestMsg]): RequestMsg => ResponseMsg = {
    handlerFor[RequestMsg, ResponseMsg](is.requestName)
  }

  def isSimpleMessage = simpleHandlerMap.contains _

  @throws(classOf[InvalidMessageException])
  def handlerFor[RequestMsg, ResponseMsg](messageName: String): RequestMsg => ResponseMsg = {
    handlerMap.get(messageName).map(_.handler).getOrElse(throw buildException(messageName)).asInstanceOf[RequestMsg => ResponseMsg]
  }

  @throws(classOf[InvalidMessageException])
  def simpleHandlerFor[RequestMsg, ResponseMsg](messageName: String): RequestMsg => ResponseMsg = {
    simpleHandlerMap.get(messageName).map(_.handler).getOrElse(throw buildException(messageName)).asInstanceOf[RequestMsg => ResponseMsg]
  }

  def buildException(messageName: String) =
    new InvalidMessageException("%s is not a registered method. Methods registered are %s".format(messageName, "(" + handlerMap.keys.mkString(",") + ";" + simpleHandlerMap.keys.mkString(",") + ")"))
}