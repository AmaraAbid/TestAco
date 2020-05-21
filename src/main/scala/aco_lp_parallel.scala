package acoProject
import scala.io.Source
import scala.util.Random
import scala.math.pow
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib
import org.scalatest.Entry
import org.apache.spark.rdd.RDD
import org.semanticweb.HermiT.tableau.TupleTableFullIndex.EntryManager



object aco_lp_parallel {
  val conf = new SparkConf().setAppName("Ant Colnoy Optimization_LP").setMaster("local[*]")// creates a local spark session
     val sc = new SparkContext(conf) // creating spark context
  //  List of Triplets (  Sub  ,  Obj  ,  Link  )
     var GList : List [ ( String , String , String ) ]    =    List ( )
  //  List of Distinct Subjects
    var Subjects  :  List[String]  =  List()
    //  List of Distinct Objects
    var Objects  :  List[String]  =  List()
    //  List of Distinct Links
    var Links  :  List[String]  =  List()
    //List of Distinct Entities 
    var DisEntity:List[String]  =  List() 
    //List of adjacency matrix
    //var adjacencyList = List[List[String]] = List(List())
    
   // var adjList : List[(String, String)] = List()
    var adjList  : List[Edge[String]] =  List()
    
     def main (args: Array[String]) 
    { 
      //Read File
      val filename = "/home/amara/SampleData"
      
      //Pass file to readData function
      readData(filename)
      
      
}
    def readData ( fname : String )     
    {
      val lembda=0.01
     val ebselon=0.01
      val lines  =  Source.fromFile(fname).getLines.toList
      for  (  i  <-  0  to  lines.length-1  ) 
      {
        var tokens  =  lines(i).split(" ")
          GList  =  (  tokens(0)  ,  tokens(1) , tokens(2)  )  ::    GList
        
          
        //Distinct Subjects
        var  s  =  Subjects.find(  _  ==  tokens(0) )
        if  (s.getOrElse("null")  ==  "null"  )
          Subjects  =  tokens(0)  ::  Subjects
        
        //Distinct Objects
        var  o  =  Objects.find(  _  ==  tokens(2) )
        if  (o.getOrElse("null")  ==  "null"  )
          Objects  =  tokens(2)  ::  Objects
          
          //Distinct Links
          var  l  =  Links.find(  _  ==  tokens(1) )
        if  (l.getOrElse("null")  ==  "null"  )
          Links  =  tokens(1)  ::  Links
          
         
         
       
        // For Distinct Entities
        DisEntity = Subjects
        for (i <- 0 to Objects.length-1)
        {
          var e = DisEntity.find( _ == Objects(i))
          if(e.getOrElse("null") == "null" )
            DisEntity = Objects(i) :: DisEntity
        }
      }
      
      
      
      println("-----------------  SUBJECTS  -------------")
        Subjects.foreach(println)
        println("-----------------  OBJECTS  -------------")
        Objects.foreach(println)
        println("-----------------  LINKS  -------------")
        Links.foreach(println)
        println("-----------------  LIST  -------------")
        GList.foreach(println)
        println("-----------------  ENTITIES  -------------")
        DisEntity.foreach(println)
        
        var zwi= DisEntity.zipWithIndex
        //vertices List
        var zip: List[(Long, String)]=zwi.map(x=>(x._2.toLong,x._1))
   zip.foreach(println)
   // Edges List
   //val edges =List(Edge(6,3,"1"),Edge(5,3,"1"),Edge(4,3,"1"),Edge(0,8,"1"))
   //val edges  =  GList.map  (  x  =>  Edge(x._1.toLong,  x._2.toLong,  "1")  )
   val edges  =  GList.map  (  x  =>  Edge(x._1.toLong,  x._2.toLong,  x._3)  )
   edges.foreach(println)
   // RDD's of vertices and edges
        val vRDD: RDD[(VertexId,String)] = sc.parallelize(zip)
        val eRDD: RDD[Edge[String]] = sc.parallelize(edges)
        val nowhere = "nowhere"
        println("vRDD is")
        vRDD.collect().foreach(println)
   
        val graph1 = Graph(vRDD, eRDD, nowhere)
         
        graph1.vertices.collect.foreach(println)
        graph1.edges.collect.foreach(println)
        
       val VerticesLength =  graph1.vertices.count().toInt
       println("Total Vertices are" +VerticesLength )
      
       //var adjListList = List.fill(VerticesLength, VerticesLength)(random.nextInt() * (100 - 0) + 0)
       //var idList = (1 to VerticesLength).toList
      //df.stat.crosstab.edges.map(x => (x._1, x._2))
         
         
      
     //  val adjList : RDD[Edge[String]] =  graph1.edges.filter {  case Edge(src,dst,link)  =>  link ==  1  }
     //adjList.collect().foreach(println)
      
       
     //   val adjList: List[(String, String)] = (graph1.edges.groupBy(_.srcId).mapValues(edges => edges.map(_.dstId))).collect().toList() 
       
      
      
       //  val adjList: List[(VertexId, VertexId)] = (graph1.edges.groupBy(_.srcId).map{case (k,v) => v.map(_.dstId) }) .collect().toList()
       
       // val adjList =  graph1.edges.filter {  case Edge(src,dst,prop)  =>  dst==  "1"  }
        //adjList.foreach(println)
     //   val adjList: RDD[(VertexId, (String))] = (graph1.edges.groupBy(_.srcId).mapValues(edges => edges.map(_.dstId))).collect().toList() 
        
    //val checkingedge= edges.contains(  Edge(._1.toLong,  ._2.toLong,  "1"))
    //val myedge = 1
   /* edges.find(x=>x == myedge) match {
       myFunction(strings.contains(myString))
       myFunction(strings.find( _ == mystring ).isDefined)
  case Some(_) => myFunction(true)
  case None => myFunction(false)
}*/
      //  edges.find(x=>x == myedge) match {
    val cc = graph1.connectedComponents()
   
    val ccTriplets = cc.triplets
    ccTriplets.foreach(println)
    
    val ccProperties = ccTriplets.map(triplet => ( triplet.srcId, triplet.dstId, triplet.attr))
    ccProperties.foreach(println)
    
    val pehromoneEntries = ccTriplets.map(triplet => ( triplet.srcId, triplet.dstId, initialPehrmonegenrator(lembda, ebselon, 1)))
      pehromoneEntries.foreach(println) 
   //  val checkingEdge =  edges.contains(myedge)
    // println(checkingEdge)
    // edges.find( _ == myedge ).isDefined  //to find 1st element in the list
       //getting only ids
     
        val ids= zip.map(y=>(y._1))
       println("only ids after maping : " +ids)
       
       val nm = graph1.partitionBy(PartitionStrategy.CanonicalRandomVertexCut)
       println("PPPPPPPPPPP " + nm)
       val tri = graph1.triangleCount().vertices
       println("CCCCCCCCCCC      " + tri)

     
           val mapped =   vRDD.mapPartitionsWithIndex{
                      // 'index' represents the Partition No
                        // 'iterator' to iterate through all elements
                        //                         in the partition
                        (index, iterator) => {
                           println("Called in Partition -> " + index)
                            
    
                           val myList = iterator.toList
                           // In a normal user case, we will do the
                           // the initialization(ex : initializing database)
                           // before iterating through each element
                           
                          
                           myList.map(x => x + " - " + index).iterator
                           
                           
                        }
                        
        
                     }
        
    mapped.collect()
    
    
    
     }
    def initialPehrmonegenrator(lembda:Double,abselon:Double,link:Int):Double={
    lembda*(link+abselon)
  }
    
}