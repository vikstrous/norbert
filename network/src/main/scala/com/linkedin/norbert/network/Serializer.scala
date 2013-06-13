package com.linkedin.norbert.network

import java.io.{OutputStream, InputStream}

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

/**
 * When you don't care about variance. IE Java users
 */
trait Serializer[RequestMsg, ResponseMsg] extends InputSerializer[RequestMsg, ResponseMsg] with OutputSerializer[RequestMsg, ResponseMsg]

// Split up for correct variance

// You can serialize a superclass of RequestMsg/ResponseMsg
trait OutputSerializer[-RequestMsg, -ResponseMsg] extends ResponseOutputSerializer[ResponseMsg] with RequestOutputSerializer[RequestMsg] {}

// You can deserealze into a subclass of RequestMsg/ResponseMsg
trait InputSerializer[+RequestMsg, +ResponseMsg] extends ResponseInputSerializer[ResponseMsg] with RequestInputSerializer[RequestMsg] {}
//
//object InputSerializer {
//  def apply[RequestMsg](ris:RequestInputSerializer[RequestMsg]) = {
//    new InputSerializer[RequestMsg, Unit](){
//      def requestName = ris.requestName
//      def requestFromBytes(bytes: Array[Byte]) = ris.requestFromBytes(bytes)
//      def responseFromBytes(bytes: Array[Byte]) {
//        return
//      }
//    }
//  }
//}

// The requestName and responseName are split up awkwardly between the serializers for backwards compatibility
// Originally InputSerializer and OutputSerializer were the only serializers and that's why requestName and
// responseName were a part of them
// However, for one way messages we need only a RequestSerializer, but we still split it for correct variance

trait RequestInputSerializer[+RequestMsg] {
  def requestName: String
  def requestFromBytes(bytes: Array[Byte]): RequestMsg
}

trait RequestOutputSerializer[-RequestMsg] {
  def requestToBytes(request: RequestMsg): Array[Byte]
}

trait ResponseInputSerializer[+ResponseMsg] {
  def responseFromBytes(bytes: Array[Byte]): ResponseMsg
}

trait ResponseOutputSerializer[-ResponseMsg] {
  def responseName: String
  def responseToBytes(response: ResponseMsg): Array[Byte]
}
