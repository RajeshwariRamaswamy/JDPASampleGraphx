package pkg.spark.hbase

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.rdd.RDD

object DeviceGraphTypes {

    sealed trait EdgeSource
  case object KO2O  extends EdgeSource
  case object Client  extends EdgeSource
  case object Tapad extends EdgeSource

  sealed trait DeviceType
  case class Browser(userAgent: String)
                      extends DeviceType
  case object IOS     extends DeviceType
  case object Android extends DeviceType

  val emptySPMap: SPMap = Map.empty[VertexId, Int]
  
  case class Adjacency(source: EdgeSource, validAt: Long )

  case class EdgeTag(adjacencies: Seq[Adjacency], distanceMap: SPMap)

  case class VertexTag(id: String, devType: DeviceType)

  def singleTag(source: EdgeSource, validAt: Long) =
    EdgeTag(Seq(Adjacency(source, validAt)), emptySPMap)
}
