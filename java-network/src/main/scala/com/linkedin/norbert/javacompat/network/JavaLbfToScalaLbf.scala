package com.linkedin.norbert
package javacompat
package network

import com.linkedin.norbert.network.partitioned.loadbalancer.{PartitionedLoadBalancerFactory => SPartitionedLoadBalancerFactory, PartitionedLoadBalancer => SPartitionedLoadBalancer}
import com.linkedin.norbert.network.client.loadbalancer.{LoadBalancerFactory => SLoadBalancerFactory, LoadBalancer => SLoadBalancer}

import com.linkedin.norbert.cluster.{Node => SNode}
import com.linkedin.norbert.network.common.{Endpoint => SEndpoint}

import com.linkedin.norbert.EndpointConversions._

class JavaLbfToScalaLbf[PartitionedId](javaLbf: PartitionedLoadBalancerFactory[PartitionedId]) extends SPartitionedLoadBalancerFactory[PartitionedId] {
  def newLoadBalancer(nodes: Set[SEndpoint]) = {
    val lb = javaLbf.newLoadBalancer(nodes)
    new SPartitionedLoadBalancer[PartitionedId] {
      def nextNode(id: PartitionedId, capability: Option[Long] = None) = {
        capability match {
          case Some(c) => Option(lb.nextNode(id, c.longValue))
          case None => Option(lb.nextNode(id))
        }
      }

      def nodesForOneReplica(id: PartitionedId, capability: Option[Long]  = None) = {
        val jMap = capability match {
          case Some(c) => lb.nodesForOneReplica(id, c.longValue)
          case None => lb.nodesForOneReplica(id)
        }
        var sMap = Map.empty[com.linkedin.norbert.cluster.Node, Set[Int]]

        val entries = jMap.entrySet.iterator
        while(entries.hasNext) {
          val entry = entries.next
          val node = javaNodeToScalaNode(entry.getKey)
          val set = entry.getValue.foldLeft(Set.empty[Int]) { (s, elem) => s + elem.intValue}

          sMap += (node -> set)
        }
        sMap
      }

      def nodesForPartitionedId(id: PartitionedId, capability: Option[Long] = None) = {
        val jSet = capability match {
          case Some(c) => lb.nodesForPartitionedId(id, c.longValue)
          case None => lb.nodesForPartitionedId(id)
        }
        var sSet = Set.empty[SNode]
        val entries = jSet.iterator
        while(entries.hasNext) {
          val node = javaNodeToScalaNode(entries.next)
          sSet += node
        }
        sSet
      }

      def nodesForPartitions(id: PartitionedId, partitions: Set[Int], capability: Option[Long] = None) = {
        val jMap =  capability match {
          case Some(c) => lb.nodesForOneReplica(id, c.longValue)
          case None => lb.nodesForOneReplica(id)
        }
        var sMap = Map.empty[com.linkedin.norbert.cluster.Node, Set[Int]]

        val entries = jMap.entrySet.iterator
        while(entries.hasNext) {
          val entry = entries.next
          val node = javaNodeToScalaNode(entry.getKey)
          val set = entry.getValue.foldLeft(Set.empty[Int]) { (s, elem) => s + elem.intValue}

          sMap += (node -> set)
        }
        sMap
      }
   }

  }

  def getNumPartitions(endpoints: Set[SEndpoint]) = javaLbf.getNumPartitions(endpoints).intValue()
}