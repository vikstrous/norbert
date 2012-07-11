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
package partitioned
package loadbalancer

import org.specs.Specification
import cluster.{InvalidClusterException, Node}
import common.Endpoint

class ConsistentHashPartitionedLoadBalancerFactorySpec extends Specification {
  case class EId(id: Int)
  implicit def eId2ByteArray(eId: EId): Array[Byte] = BigInt(eId.id).toByteArray

  class EIdDefaultLoadBalancerFactory(numPartitions: Int, serveRequestsIfPartitionMissing: Boolean) extends DefaultPartitionedLoadBalancerFactory[EId](serveRequestsIfPartitionMissing) {
    protected def calculateHash(id: EId) = HashFunctions.fnv(id)

    def getNumPartitions(endpoints: Set[Endpoint]) = numPartitions
  }

  def toEndpoints(nodes: Set[Node], failingNodes: Set[Node] = Set.empty[Node]) = nodes.map(n => new Endpoint {
      def node = n

      def canServeRequests = !failingNodes.contains(n)
    })

  val loadBalancerFactory = new EIdDefaultLoadBalancerFactory(5, true)

  "ConsistentHashPartitionedLoadBalancer" should {
    "nextNode returns the correct node for 1210" in {
      val nodes = Set(
        Node(0, "localhost:31313", true, Set(0, 1)),
        Node(1, "localhost:31313", true, Set(1, 2)),
        Node(2, "localhost:31313", true, Set(2, 3)),
        Node(3, "localhost:31313", true, Set(3, 4)),
        Node(4, "localhost:31313", true, Set(0, 4)))

      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))
      lb.nextNode(EId(1210)) must beSome[Node].which(List(Node(0, "localhost:31313", true, Set(0, 1)),
        Node(4, "localhost:31313", true, Set(0, 4))) must contain(_))
    }


    "throw InvalidClusterException if all partitions are unavailable" in {
      val nodes = Set(
        Node(0, "localhost:31313", true, Set[Int]()),
        Node(1, "localhost:31313", true, Set[Int]()))

      new EIdDefaultLoadBalancerFactory(2, false).newLoadBalancer(toEndpoints(nodes)) must throwA[InvalidClusterException]
    }

    "throw InvalidClusterException if one partition is unavailable, and the LBF cannot serve requests in that state, " in {
      val nodes = Set(
        Node(0, "localhost:31313", true, Set(1)),
        Node(1, "localhost:31313", true, Set[Int]()))

      new EIdDefaultLoadBalancerFactory(2, true).newLoadBalancer(toEndpoints(nodes)) must not (throwA[InvalidClusterException])
      new EIdDefaultLoadBalancerFactory(2, false).newLoadBalancer(toEndpoints(nodes)) must throwA[InvalidClusterException]
    }

    "nodesForPartitionedId returns all the correct nodes for 1210" in {
      val nodes = Set (
        Node(0, "localhost:12345", true, Set(0,1,2)),
        Node(1, "localhost:23451", true, Set(1,2,3)),
        Node(2, "localhost:34512", true, Set(2,3,4)),
        Node(3, "localhost:45123", true, Set(3,4,0)),
        Node(4, "localhost:51234", true, Set(4,0,1))
      )
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))
      lb.nodesForPartitionedId(EId(1210)) must haveTheSameElementsAs (Set(Node(0, "localhost:12345", true, Set(0,1,2)),
                                                         Node(3, "localhost:45123", true, Set(3,4,0)),
                                                         Node(4, "localhost:51234", true, Set(4,0,1))))
     }

    "nodesForPartitionedId returns all nodes regardless if they can serve requests" in {
      val nodes = Set (
        Node(0, "localhost:12345", true, Set(0,1,2)),
        Node(1, "localhost:23451", true, Set(1,2,3)),
        Node(2, "localhost:34512", true, Set(2,3,4)),
        Node(3, "localhost:45123", true, Set(3,4,0)),
        Node(4, "localhost:51234", true, Set(4,0,1))
      )
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes, Set(nodes.last)))
      lb.nodesForPartitionedId(EId(1210)) must haveTheSameElementsAs (Set(Node(0, "localhost:12345", true, Set(0,1,2)),
                                                                          Node(3, "localhost:45123", true, Set(3,4,0)),
                                                                          Node(4, "localhost:51234", true, Set(4,0,1))))
     }
  }
}
