package pkg.spark.hbase
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.rdd.RDD
import DeviceGraphTypes._

object GraphxComponents  {
  def main(args: Array[String]) {
 val conf = new SparkConf().setAppName("GraphX device graph demo")
    .setMaster("local[2]")

  val sc = new SparkContext(conf)

  val deviceInfo: RDD[(VertexId, VertexTag)] =
    sc.parallelize(Array(
      (11L, VertexTag("A", Android)),
      (22L, VertexTag("B", IOS)),
      (13L, VertexTag("C", Browser("Mozilla/5.0"))),
      (18L, VertexTag("D", Android)),
      (12L, VertexTag("E", IOS))))

  val deviceAdjacency: RDD[Edge[EdgeTag]] =
    sc.parallelize(Array(
      Edge(11L, 22L, singleTag(KO2O, 100L)),
      Edge(22L, 36L, singleTag(Tapad, 20L)),
      Edge(12L, 21L, singleTag(KO2O, 100L)),
      Edge(21L, 34L, singleTag(Tapad, 100L)),
      Edge(21L, 35L, singleTag(Tapad, 50L)),
      Edge(21L, 41L, singleTag(Client, 50L)),
      Edge(13L, 41L, singleTag(KO2O, 50L)),
      Edge(18L, 1L, singleTag(KO2O, 50L)),
      Edge(1L, 7L, singleTag(Client, 50L))
  ))
  
  
  val newdeviceAdjacency: RDD[Edge[EdgeTag]] =
    sc.parallelize(Array(
      Edge(11L, 25L, singleTag(KO2O, 60L)),
      Edge(24L, 36L, singleTag(Tapad, 40L)),
      Edge(12L, 28L, singleTag(KO2O, 50L)),
      Edge(13L, 35L, singleTag(Tapad, 500L))

  ))

  //Union Of both RDD for the insertion of new devices added recently
  
  val CurrentDeviceAdjacency = deviceAdjacency.union(newdeviceAdjacency)
  
  
  // if device is missing in current but appears before,add the device//
  
  //TODO
  // Get the Edges for that time period
  
  // Returns the entries in Old Edges RDD that have no corresponding key in New EdgesRDD.

  
  
  val graph = Graph(deviceInfo, CurrentDeviceAdjacency)
  
  
  
 // BEGIN connected components

//  val ccGraph = graph.connectedComponents()
//  println(ccGraph.vertices.collect().toList)

  // END connected components
 

 
  val filteredIds : Seq[VertexId] = Seq(11, 12, 13, 18)


  // BEGIN weighted Shortest paths
  //
  // See the unweighted version in the Spark GraphX library:
  // https://github.com/apache/spark/blob/master/graphx/src/main/scala/org/apache/spark/graphx/lib/ShortestPaths.scala
  //
  // See the Pregel API docs at
  // http://spark.apache.org/docs/latest/graphx-programming-guide.html#pregel-api

  val OneWeek: Long = 7 * 24 * 60 * 60 * 1000L

  val weights = graph.mapTriplets { edge =>
    val adjacencies = edge.attr.adjacencies.
      sortBy(- _.validAt) // Newest adjacencies are sorted first.

    // Assign weight based on adjacencies
    // Rules can be as complex as we want.
    // The rules below are made up just to prove the point.

    if( adjacencies.isEmpty )
      Int.MaxValue // Absolute lowest weight if no adjacencies.
    else if( adjacencies.exists(_.source == KO2O) )
      // Highest weight if at least one adjacency from KO2O has ever existed.
      1
    else if( System.currentTimeMillis - adjacencies.head.validAt > OneWeek )
      // Lower weight if the most recent adjacency is too old
      4
    else if( adjacencies.exists(_.source == Client) )
      2
    else if( adjacencies.forall(_.source == Tapad) )
      // Not just one but all adjacencies come from Tapad
      4
    else
      // Even lower weight in all other cases
      16
  }

  def incrementMap(spmap: SPMap, distance: Int): SPMap
    = spmap.map { case (v, d) => v -> (d + distance) }

  def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
    }.toMap

  val spGraph = weights.mapVertices { (vid, attr) =>
    // when the filtered ids are joined to the weights we can
    // check the attr instead of checking this sequence
    if (filteredIds.contains(vid))
      Map(vid -> 0)
    else
      emptySPMap
  }

  val initialMessage = emptySPMap

  def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
    addMaps(attr, msg)
  }

  def sendMessage(edge: EdgeTriplet[SPMap, Int]): Iterator[(VertexId, SPMap)] = {
    val dstAttr = incrementMap(edge.dstAttr, edge.attr)
    val srcAttr = incrementMap(edge.srcAttr, edge.attr)

    if (edge.srcAttr != addMaps(dstAttr, edge.srcAttr)) Iterator((edge.srcId, dstAttr))
    else if (edge.dstAttr != addMaps(srcAttr, edge.dstAttr)) Iterator((edge.dstId, srcAttr))
    else Iterator.empty
  }

  val distances = Pregel(spGraph, initialMessage, Int.MaxValue, EdgeDirection.Either)(vertexProgram, sendMessage, addMaps)

  println(distances.vertices.take(20).mkString(" "))
  }
}

