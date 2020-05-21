package acoProject
import scala.io.Source
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala. sys. process. _
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.Matrix
import scala.math.pow
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib
import org.scalatest.Entry
import org.semanticweb.HermiT.tableau.TupleTableFullIndex.EntryManager
import org.apache.spark.rdd.RDD
import java.io.PrintWriter

object Aco_LP_H {
  var GList : List [(String, String, String)] = List()
  //var GList : List [(String, String)] = List()
  val conf = new SparkConf().setAppName("Ant Colnoy Optimization_LP").setMaster("local[*]")// creates a local spark session
     val sc = new SparkContext(conf) // creating spark context
  
    
     def main(args : Array[String]) {
    
 /*   val conf1 = new Configuration()
    conf1.set("fs.defaultFS", "hdfs://localhost:54310")
    val fs= FileSystem.get(conf1)
    val output = fs.create(new Path("/ACODATA.txt"))
    val writer = new PrintWriter("hdfs://localhost:50070/ACODATA.txt")
try {
    writer.write("this is a test") 
    writer.write("\n")
}
finally {
    writer.close()
    println("Closed!")
}
println("Done!")
*/



val lines = sc.textFile("hdfs://localhost:50070/user/SampleData").collect().toList
  println("lines are")
  lines.foreach(println)
  for (i <- 0 to lines.length-1){
   var tokens = lines(i).split(" ")
   GList = ( tokens(0) , tokens(1) , tokens(2) ) :: GList
   
   
  }
  println("GraphList: " + GList) 
  
  //lines.foreach(println)
 // println(lines)
   // val lines1 = Source.fromFile(lines).getLines().toList
    
    
     
  }
  
  
}