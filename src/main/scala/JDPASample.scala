/* SampleApp.scala:
   This application simply counts the number of lines that contain "val" from itself
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
 
object SimpleApp {
  def main(args: Array[String]) {
    val txtFile = "/home/osboxes/SampleApp/src/main/scala/SampleApp.scala"
    val conf = new SparkConf().setAppName("Sample Application")
    val sc = new SparkContext(conf)
    val txtFileLines = sc.textFile(txtFile , 2).cache()
    val numAs = txtFileLines .filter(line => line.contains("val")).count()
    println("Lines with val: %s".format(numAs))
  }
}
