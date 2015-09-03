package pkg.spark.graphx

import org.specs2._
import org.specs2.matcher._
import DeviceGraphTypes._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.rdd.RDD

class WeightRulesSpec extends Specification  { 

     def is = s2"""
   This is a specification to assign highest weight to KO2O adjacencies

 The 'weight'  should
   less than 5 for more weightage                                     $CheckWeightage
                                                                      """
 
  val adjacencies = Seq(Adjacency(Tapad,10L),Adjacency(KO2O,500L),Adjacency(Tapad,1000L))
   val result = GraphxComponents.adjacencyRules(adjacencies)
   
   def CheckWeightage = result must beEqualTo(1)
 
}

