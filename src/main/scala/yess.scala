import org.apache.spark._


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Map
import scala.util.Random
import scala.math.BigDecimal
import scala.math.BigInt

object yess {
  def main(args: Array[String]){
    println("Hello")
    
    println("Enter Number of students")
    val stu  = scala.io.StdIn.readInt()
    println("Enter Number of Subjects")
    val sub =  scala.io.StdIn.readInt()
    println("Enter Number of Partitions")
    val p =  scala.io.StdIn.readInt()
    println("Enter Number of id")
    val id =  scala.io.StdIn.readInt()
    val random = new Random()
    
    var marksList = List.fill(stu, sub)(random.nextInt() * (100 - 0) + 0)
    println("marks list:")
    marksList.foreach(println)
    var idList = (1 to stu).toList
    println("ids list:")
    //var iList = (idList, (BigDecimal(idsList).setScale(4, BigDecimal.RoundingMode.HALF_UP).toIntExact)):: idList
    idList.foreach(println)
    var stuList = idList.zip(marksList)
    stuList.foreach(println)
    
    
   
    
   val Conf = new SparkConf().setAppName("my Spark RDD").setMaster("local[*]")
    val sc = new SparkContext(Conf)
    //val r  =  sc.parallelize(  List(  1,4,2,6,8,9,3,55,77,88,22,11  )  )
    //r.foreach(println)
    val marksIdrdd = sc.parallelize(stuList, p)
    marksIdrdd.foreach(println)
    val marksrdd = sc.parallelize(marksList,p)
    marksrdd.foreach(println)
   
  // val L3 = marksrdd.map(v => (v._1).sum())
   //val L4 = (L3 / sub) * 100
   //L3.foreach(println)
   //finalData.foreach(println)
   // val rdd2 = 
    
  /* def marks = x {
      marks match{
        case (marks.map(m => (m._2._ > = 85)) => println("grade is 4")
        case (marks.map(m => (m._2._ > = 70)) => println("grade is 3")
        case (marks.map(m => (m._2 > = 58)) => println("grade is 2")
        case (marks.map(m => (m._2 > = 50)) => println("grade is 1")
        case (marks.map(m => (m._2 < = 50)) => println("grade is 0")
      }
    }*/
    
    
   //marks.foreach(println)
    //marks.foreach(println)
    //val rdd2array = sc.broadcast(x.collect())
    //val result = marksrdd.map(line => marks(line, rdd2array))
    //result.foreach(println)
}

}