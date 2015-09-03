package pkg.spark.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.rdd.RDD
import DeviceGraphTypes._
import java.util.Date
import java.util.Calendar


object  GraphxComponents {
  
  val calender = Calendar.getInstance();
 
   val currentTimestamp = calender.getTime();
   val today = currentTimestamp.getTime();
   calender.add(Calendar.DATE, -1)
   val previousdayTimestamp = calender.getTime();
   val yesterday = previousdayTimestamp.getTime();
   
  calender.add(Calendar.WEEK_OF_MONTH, 0)
  val currentWeekTimestamp = calender.getTime();
  val currentWeek = currentWeekTimestamp.getTime();
 
  calender.add(Calendar.WEEK_OF_MONTH, -1)
  val oneWeekBeforeTimestamp = calender.getTime();
  val oneWeekBefore = oneWeekBeforeTimestamp.getTime();
  
  calender.add(Calendar.WEEK_OF_MONTH, -1)
  val twoWeekBeforeWeekTime = calender.getTime();
  val twoWeekBefore = twoWeekBeforeWeekTime.getTime();
  
def adjacencyRules(adjacencies : Seq[Adjacency]) : Int
     = {
        if( adjacencies.isEmpty )
           Int.MaxValue // Absolute lowest weight if no adjacencies.
         else if( adjacencies.exists(_.source == KO2O) )
           // Highest weight if at least one adjacency from KO2O has ever existed.
          1
          else if( System.currentTimeMillis - adjacencies.head.validAt > currentWeek )
           // Lower weight if the most recent adjacency is too old
          4
           
        else if(adjacencies.exists(adj => adj.validAt < currentWeek 
              && adj.validAt > oneWeekBefore  &&  adj.validAt <  twoWeekBefore))
           // if edge seen this week , not in the previous week but there 2 weeks before
           20
      
         else if( adjacencies.exists(_.source == Client) )
           2
           
        else if( adjacencies.forall(_.source == Tapad) )
           // Not just one but all adjacencies come from Tapad
          4
         else
           // Even lower weight in all other cases
          16
}

 
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
      Edge(11L, 22L, EdgeTag(Seq(
          Adjacency(KO2O,today),
          Adjacency(KO2O,oneWeekBefore),
          Adjacency(KO2O,yesterday),
          Adjacency(Tapad,today),
          Adjacency(Tapad,yesterday)))),
     Edge(22L, 36L, EdgeTag(Seq(
          Adjacency(Tapad,today),
          Adjacency(Tapad,yesterday),
          Adjacency(Tapad,oneWeekBefore)))),
     Edge(12L, 21L, EdgeTag(Seq(
          Adjacency(KO2O,today),
          Adjacency(Tapad,today),
          Adjacency(Tapad,yesterday)))),
     Edge(21L, 34L, EdgeTag(Seq(
          Adjacency(KO2O,today)))),
     Edge(21L, 41L, EdgeTag(Seq(
          Adjacency(Tapad,today),
          Adjacency(Tapad,oneWeekBefore)))),
    Edge(13L, 41L, EdgeTag(Seq(
          Adjacency(Client,today),
          Adjacency(KO2O,today),
          Adjacency(Tapad,today)))),
    Edge(18L, 1L, EdgeTag(Seq(
          Adjacency(KO2O,today),
          Adjacency(KO2O,twoWeekBefore))))
  ))
  
  
  val newdeviceAdjacency: RDD[Edge[EdgeTag]] =
    sc.parallelize(Array(
      Edge(11L, 25L, singleTag(KO2O, yesterday)),
      Edge(24L, 36L, singleTag(Tapad, today)),
      Edge(12L, 28L, singleTag(KO2O, yesterday)),
      Edge(13L, 35L, singleTag(Tapad, today))

  ))
  
  //Union Of both RDD for the insertion of new devices added recently
  
  val currentDeviceAdjacency = deviceAdjacency.union(newdeviceAdjacency)
  
   
  val graph = Graph(deviceInfo, currentDeviceAdjacency)
 
  val filteredIds : Seq[VertexId] = Seq(11, 12, 13, 18,21)


  // BEGIN weighted Shortest paths
  //
  // See the unweighted version in the Spark GraphX library:
  // https://github.com/apache/spark/blob/master/graphx/src/main/scala/org/apache/spark/graphx/lib/ShortestPaths.scala
  //
  // See the Pregel API docs at
  // http://spark.apache.org/docs/latest/graphx-programming-guide.html#pregel-api

  //val OneWeek: Long = 7 * 24 * 60 * 60 * 1000L // 7 days * 24 Hrs *60 Min * 60 Sec * 1000 Ms
   




  val weights =  graph.mapTriplets { edge => adjacencyRules(edge.attr.adjacencies.
      sortBy(- _.validAt) )}
  


  def incrementMap(spmap: SPMap, distance: Int): SPMap
    = spmap.map { case (v, d) => v -> (d + distance) }

  def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
    }.toMap

  val spGraph = weights.mapVertices { (vid, attr) =>
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

