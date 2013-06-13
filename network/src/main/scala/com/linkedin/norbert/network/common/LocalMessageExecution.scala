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
package common

import server.MessageExecutorComponent
import cluster.{Node, ClusterClientComponent}

trait LocalMessageExecution extends BaseNetworkClient {
  this: MessageExecutorComponent with ClusterClientComponent with ClusterIoClientComponent =>

  val myNode: Node

  override protected def doSendRequest[RequestMsg](requestCtx: SimpleMessage[RequestMsg])
                                                               (implicit is: RequestInputSerializer[RequestMsg], os: RequestOutputSerializer[RequestMsg]): Unit = {
    if(requestCtx.node == myNode) requestCtx match {
      case msg: Request[_, _] => messageExecutor.executeMessage(requestCtx.message, msg.callback)
      case _ => messageExecutor.executeMessage(requestCtx.message, null)
    }
    else super.doSendRequest(requestCtx)
  }
}
