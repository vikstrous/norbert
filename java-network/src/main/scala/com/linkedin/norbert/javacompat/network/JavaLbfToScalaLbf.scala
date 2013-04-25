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
      def nextNode(id: PartitionedId, capability: Option[Long] = None, permanentCapability: Option[Long]) = {
        (capability, permanentCapability) match {
          case (Some(c),Some(pc)) => Option(lb.nextNode(id, c.longValue, pc.longValue))
          case (None, Some(pc)) => Option(lb.nextNode(id, 0L, pc.longValue))
          case (Some(c), None) => Option(lb.nextNode(id, c.longValue, 0L))
          case (None, None) => Option(lb.nextNode(id))
        }
      }

      def nodesForOneReplica(id: PartitionedId, capability: Option[Long]  = None, permanentCapability: Option[Long]) = {
        val jMap = (capability, permanentCapability) match {
          case (Some(c),Some(pc)) => lb.nodesForOneReplica(id, c.longValue, pc.longValue)
          case (Some(c), None) => lb.nodesForOneReplica(id, c.longValue, 0L)
          case (None, Some(pc)) => lb.nodesForOneReplica(id, 0L, pc.longValue)
          case (None, None) => lb.nodesForOneReplica(id)
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

      def nodesForPartitionedId(id: PartitionedId, capability: Option[Long] = None, permanentCapability: Option[Long]) = {
        val jSet = (capability, permanentCapability) match {
          case (Some(c), Some(pc)) => lb.nodesForPartitionedId(id, c.longValue, pc.longValue)
          case (Some(c), None) => lb.nodesForPartitionedId(id, c.longValue, 0L)
          case (None, Some(pc)) => lb.nodesForPartitionedId(id, 0L, pc.longValue)
          case (None, None) => lb.nodesForPartitionedId(id, 0L, 0L)
        }
        var sSet = Set.empty[SNode]
        val entries = jSet.iterator
        while(entries.hasNext) {
          val node = javaNodeToScalaNode(entries.next)
          sSet += node
        }
        sSet
      }

      def nodesForPartitions(id: PartitionedId, partitions: Set[Int], capability: Option[Long] = None, permanentCapability: Option[Long]) = {
        val jMap =  (capability, permanentCapability) match {
          case (Some(c), Some(pc)) => lb.nodesForOneReplica(id, c.longValue, pc.longValue)
          case (Some(c), None) => lb.nodesForOneReplica(id, c.longValue, 0L)
          case (None, Some(pc)) => lb.nodesForOneReplica(id, 0L, pc.longValue)
          case (None, None) => lb.nodesForOneReplica(id)
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