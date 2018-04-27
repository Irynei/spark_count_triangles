/**
  * Created by irko on 26.04.18.
  */
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}

object Main {
  def main(args: Array[String]): Unit = {
    val filename = if(args.isEmpty) "src/main/resources/followers.txt"  else args(0)
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Simple Spark Project")
      .getOrCreate()

    val sc = spark.sparkContext
    val graph = GraphLoader.edgeListFile(sc, filename, true)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    // Triangle count from lib
//    val triCounts = graph.triangleCount().vertices

    // custom Triangle Count
    val triCounts = CustomTriangleCount.customRun(graph).vertices
//    println(s"Number of Triangles: ${triCounts.values.sum() / 3}")
//    println(s"Number of Triangles: ${triCounts.map(_._2).sum() / 3}")
    println(s"Number of Triangles: ${triCounts.map(_._2).reduce(_ + _) / 3}")
  }
}
