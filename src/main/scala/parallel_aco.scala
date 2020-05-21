package acoProject
import scala.io.Source
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.Matrix
import scala.math.pow
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib
import org.scalatest.Entry
import org.semanticweb.HermiT.tableau.TupleTableFullIndex.EntryManager
import org.apache.spark.rdd.RDD

object parallel_aco {
  val conf = new SparkConf().setAppName("Ant Colnoy Optimization_LP").setMaster("local[*]")// creates a local spark session
     val sc = new SparkContext(conf) // creating spark context
  var GList : List [ ( String , String ) ]    =    List ( )
  var tripletList :List [( String, String, String ) ] = List()
  //  List of Triplets (  Sub  ,  Obj  ,  Link  )
    var Triplets : List [ ( String , String , String ) ]    =    List ( )
    //  List of Distinct Subjects
    var Subjects  :  List[String]  =  List()
    //  List of Distinct Objects
    var Objects  :  List[String]  =  List()
    //  List of Distinct Links
    var Links  :  List[String]  =  List()
    var pheromone: List[(String , String)] = List()
   
  def main(args : Array[String]) {
    
    
     val Filename= "/home/amara/SampleData"// data set
     loadFile(Filename)
     
    
     
  }
 def loadFile ( fname : String )    
    {
      val lines  =  Source.fromFile(fname).getLines.toList
       
      for  (  i  <-  0  to  lines.length-1  )
      {
        var tokens  =  lines(i).split(" ")
        Triplets  =  (  tokens(0).trim()  ,  tokens(1).trim()  ,  tokens(2).trim()  )  ::    Triplets
      
       /* var  s  =  Subjects.find(  _  ==  tokens(0) )
        if  (s.getOrElse("null")  ==  "null"  )
          Subjects  =  tokens(0)  ::  Subjects
       
        var  o  =  Objects.find(  _  ==  tokens(1) )
        if  (o.getOrElse("null")  ==  "null"  )
          Objects  =  tokens(1)  ::  Objects
         
        var  l  =  Links.find(  _  ==  tokens(2) )
        if  (l.getOrElse("null")  ==  "null"  )
          Links  =  tokens(2)  ::  Links*/
          
       
      }
       var Subjects=Triplets.map(_._1)
       var Objects=Triplets.map(_._1)
        println("-----------------  SUBJECTS  -------------")
        Subjects.foreach(println)
        println("-----------------  LINKS  -------------")
        Links.foreach(println)
        println("-----------------  OBJECTS  -------------")
        Objects.foreach(println)
        println("-----------------  TRIPLETS  -------------")
        Triplets.foreach(println)
        
   var Links1 = Links.map(_.toInt)//.distinct 
   var Objects1 = Objects.map(_.toInt)//.distinct 
   var Subjects1 = Subjects.map(_.toInt)//.distinct
   println("objects=..........")
   Objects1.foreach(println)
   // println("o"+Objects1.size+"......."+Triplets.length+"..........s"+Subjects1.size)
         var AdjacencyMatrix = List[(Int, Int, Int)]()
         //val AdjacencyMatrix: Matrix = Matrices.dense(Subjects.length, Objects.length, Array(1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0,1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0,1.0, 0.0, 0.0, 1.0, 1.0, 1.0 ))
      for(i <- 0 to Triplets.length-1)
    {
      //val relationIndex = Links.indexOf(Triplets(i)._3)
      //val subjectIndex = Subjects.indexOf(Triplets(i)._1)      
      //val objectIndex = Objects.indexOf(Triplets(i)._2)
        AdjacencyMatrix=AdjacencyMatrix:::List((Subjects1(i),Objects1(i),1))
      //AdjacencyMatrix(relationIndex)(subjectIndex)(objectIndex) = 1 
        
    }
        
        for(relationIndex <-0 to Links.length-1)
    {
      //Formating
      print("-----------------------------------")
      for(dashes <- 0 to Links(relationIndex).length())
        print("-")
      print("-------------------\n")
      println("---------------Matrix For Relation \'" + Links(relationIndex) + "\' is---------------")
      print("-----------------------------------")
      for(dashes <- 0 to Links(relationIndex).length())
        print("-")
      print("-------------------\n")
      
      print("Subject\\Objects  ")
      
      for(objectIndex <-0 to Objects.length-1)
      {
        print(Objects(objectIndex)+ "  ")
      }
      println()
      
      for(subjectIndex <-0 to Subjects.length-1)
      {
        print(Subjects(subjectIndex))
        for(spaces <- 0 to 17-Subjects(subjectIndex).length())
            print(" ")
        for(objectIndex <- 0 to Objects.length-1)
        {          
          //print(AdjacencyMatrix(relationIndex)(subjectIndex)(objectIndex))
          for(spaces <- 1 to Objects(objectIndex).length()+1)
            print(" ")
        }
        println()
      }
      println()
    }
       //val n = toRDD(AdjacencyMatrix)
        
    
      
        //adjRDD.collect().foreach(a => println(a))     
      
        val adjRDD = sc.parallelize(AdjacencyMatrix, 4) 
       
        var vv=adjRDD.collect().foreach(println)//.foreach(a => println(a))
         println("AAAAAAAAAAAAAA")
        val adjRDDCount = adjRDD.count().toInt
        val pheromone = sc.parallelize(0 until adjRDDCount, 4) 
        
        
       
    val mapped =   adjRDD.mapPartitionsWithIndex{
                      // 'index' represents the Partition No
                        // 'iterator' to iterate through all elements
                        //                         in the partition
                        (index, iterator) => {
                           println("Called in Partition -> " + index)
                            
    
                           val myList = iterator.toList
                           // In a normal user case, we will do the
                           // the initialization(ex : initializing database)
                           // before iterating through each element
                          myList.foreach(println)
                          
                           myList.map(x => x + " - " + index).iterator
                           
                         
                        }
                        
        
                     }
        
    mapped.collect()
    }
 


 def pheromoneCalculate(lambda:Double,abselon:Double,link:Int):Double={
    lambda*(link+abselon)
  }
 
  /*def toRDD (m:Matrix) : RDD[Vector] = {
    val columns = m.toArray.grouped(m.numRows)
    val rows = columns.toSeq.transpose
    val vectors = rows.map(row => new DenseVector(row.toArray))
    sc.parallelize(vectors, 2)
    
  } */     
        
}
    
  