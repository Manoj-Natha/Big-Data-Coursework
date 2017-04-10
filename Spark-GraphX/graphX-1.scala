// Databricks notebook source exported at Fri, 4 Nov 2016 20:41:07 UTC
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
// import classes required for using GraphX
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators


//-----------------------------------------Step I:-----------------------------------------

// define the CollaborationNetwork Schema
case class CollaborationNetwork(collaborator1:Long, collaborator2:Long)

// function to parse input into CollaborationNetwork class
def parseCollaborationNetwork(str: String): CollaborationNetwork = {
    val line = str.split("\\s+")
    CollaborationNetwork(line(0).toLong, line(1).toLong)
}

// load the data into a RDD
val textRDD = sc.textFile("/FileStore/tables/ixbrsjcu1478149225484/data.txt")

// parse the RDD of input lines into an RDD of CollaborationNetwork classes
val CollaboratorsRDD = textRDD.map(parseCollaborationNetwork).cache()


//-----------------------------------------Step II:-----------------------------------------
val Collaborators = CollaboratorsRDD.map(CollaborationNetwork => (CollaborationNetwork.collaborator1,"collaborator "+CollaborationNetwork.collaborator1.toString)).distinct

val nowhere = "nowhere"

val Network = CollaboratorsRDD.map(CollaborationNetwork => ((CollaborationNetwork.collaborator1, CollaborationNetwork.collaborator2), 1)).distinct

val edges = Network.map {
 case ((org_id, dest_id), distance) =>Edge(org_id.toLong, dest_id.toLong, distance) }

 
// define the graph
val graph = Graph(Collaborators, edges, nowhere)

// graph vertices
//graph.vertices.take(2)

// How many Collaborators?
//val numNodes = graph.numVertices

//-----------------------------------------Step III:-----------------------------------------

//-----------------------------------------Step III - part a:-----------------------------------------
// Define a reduce operation to compute the highest degree vertex
def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
 if (a._2 > b._2) a else b
}
val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)

//-----------------------------------------Step III - part b:-----------------------------------------
val maxInDegree: (VertexId, Int) = graph.inDegrees.reduce(max)


//-----------------------------------------Step III - part c:-----------------------------------------

// use pageRank
val ranks = graph.pageRank(0.1).vertices

// join the ranks  with the map of Collaborators id to name
val temp= ranks.join(Collaborators)

//temp.take(1)
// Array((15370,(0.5365013694244737,TUL)))

// sort by ranking
val temp2 = temp.sortBy(_._2._1, false)
//temp2.take(5)

// get just the Collaborators names
val impCollaborators =temp2.map(_._2._2)
// Top 5 impCollaborators 
impCollaborators.take(5)

//-----------------------------------------Step III - part d:-----------------------------------------

val cc = graph.connectedComponents().vertices

val ccByCollaboratorsname = Collaborators.join(cc).map {
  case (id, (username, cc)) => (username, cc)
}

// Print the result
println(ccByCollaboratorsname.collect().mkString("\n"))

//-----------------------------------------Step III - part e:-----------------------------------------

/*
val triCounts = graph.triangleCount().vertices
// Join the triangle counts with the Collaborators
val triCountByCollaboratorsname = Collaborators.join(triCounts).map { case (id, (username, tc)) =>
  (username, tc)
}
// Print the result
println(triCountByCollaboratorsname.collect().mkString("\n"))
*/


/*

execution of part 5 gives error as below: 

org.apache.spark.SparkException: Job aborted due to stage failure: Task 1 in stage 2318.0 failed 1 times, 
most recent failure: Lost task 1.0 in stage 2318.0 (TID 828, localhost): java.lang.IllegalArgumentException: 
requirement failed: Invalid initial capacity

*/