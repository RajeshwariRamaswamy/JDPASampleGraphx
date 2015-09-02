package pkg.spark.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object GraphxTest {

    def main(args: Array[String]) {
	val conf = new SparkConf().setAppName("GraphX device graph demo")
			.setMaster("local[2]")

			val sc = new SparkContext(conf)

	val deviceInfo: RDD[(VertexId, (String, String))] =
	sc.parallelize(Array((1L, ("A", "Android")),
			(2L, ("B", "Mobile")),
			(3L, ("C", "PC")), 
			(4L, ("D", "MobileApp")),
			(5L, ("E", "Tablet"))))


	val deviceAdjacency: RDD[Edge[String]] =
			sc.parallelize(Array(Edge(1L, 2L, "KO2O"),    Edge(1L, 3L, "Tapad"),
					Edge(2L, 3L,"KO20"), Edge(2L, 4L,"KO2O"), Edge(4L, 5L,"Client")))


					val graph = Graph(deviceInfo, deviceAdjacency)

					println(graph.vertices.count)
					println(graph.edges.count)
					println(graph.vertices.first)
					val facts: RDD[String] =
					graph.triplets.map(triplet =>
					triplet.srcAttr._1 + " is related in strength value as " + triplet.attr + " of " + triplet.dstAttr._1)
					facts.collect.foreach(println(_))
					println("Neighnour Id List:" + graph.collectNeighborIds(EdgeDirection.Out))
					println("Neighnours List:" +graph.collectNeighbors(EdgeDirection.Out))
					println("Vertices List:" +graph.vertices.collect().toList)

					val ccGraph = graph.connectedComponents()

					println("Connected Components:"+ ccGraph.vertices.collect().toList)
    }


}