package com.linkedin.norbert
package javacompat
package network

import com.linkedin.norbert.network.partitioned.loadbalancer.{PartitionedLoadBalancerFactory => SPartitionedLoadBalancerFactory}
import com.linkedin.norbert.EndpointConversions._
import javacompat.cluster.Node
import javacompat._


class ScalaLbfToJavaLbf[PartitionedId](scalaLbf: SPartitionedLoadBalancerFactory[PartitionedId]) extends PartitionedLoadBalancerFactory[PartitionedId] {

  def newLoadBalancer(endpoints: java.util.Set[Endpoint]) = {
    val scalaBalancer = scalaLbf.newLoadBalancer(endpoints)

    new PartitionedLoadBalancer[PartitionedId] {
      def nodesForOneReplica(id: PartitionedId) = nodesForOneReplica(id, 0L, 0L)

      def nodesForOneReplica(id: PartitionedId, capability: java.lang.Long, permanentCapability: java.lang.Long) = {
        val replica = scalaBalancer.nodesForOneReplica(id, capability, permanentCapability)
        val result = new java.util.HashMap[Node, java.util.Set[java.lang.Integer]](replica.size)

        replica.foreach { case (node, partitions) =>
          result.put(node, partitions)
                        }

        result        
      }

      def nextNode(id: PartitionedId) = nextNode(id, 0L, 0L)

      def nextNode(id: PartitionedId, capability: java.lang.Long, permanentCapability: java.lang.Long) =  {
        scalaBalancer.nextNode(id, capability, permanentCapability) match {
          case Some(n) =>n
          case None => null
        }
      }

      def nodesForPartitionedId(id: PartitionedId) = nodesForPartitionedId(id, 0L, 0L)

      def nodesForPartitionedId(id: PartitionedId, capability: java.lang.Long, permanentCapability: java.lang.Long) = {
        val set = scalaBalancer.nodesForPartitionedId(id, capability, permanentCapability)
        val jSet = new java.util.HashSet[Node]()
        set.foldLeft(jSet) { case (jSet, node) => {jSet.add(node); jSet} }
        jSet
      }

      def nodesForPartitions(id: PartitionedId, partitions: java.util.Set[java.lang.Integer]) = nodesForPartitions(id, partitions, 0L, 0L)

      def nodesForPartitions(id: PartitionedId, partitions:java.util.Set[java.lang.Integer], capability: java.lang.Long, permanentCapability: java.lang.Long) =  {
        val replica = scalaBalancer.nodesForPartitions(id, partitions, capability, permanentCapability)
        val result = new java.util.HashMap[Node, java.util.Set[java.lang.Integer]](replica.size)

        replica.foreach { case (node, partitions) =>
          result.put(node, partitions)
                        }

        result
      }

      implicit def toOption(capability: java.lang.Long) : Option[Long] = {
        if (capability.longValue == 0L) None
        else Some(capability.longValue)
      }
    }
    

  }

  def getNumPartitions(endpoints: java.util.Set[Endpoint]) = {
    scalaLbf.getNumPartitions(endpoints)
  }
}