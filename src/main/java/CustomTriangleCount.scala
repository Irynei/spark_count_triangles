import scala.reflect.ClassTag
import org.apache.spark.graphx.lib.TriangleCount
import org.apache.spark.graphx.Graph

object CustomTriangleCount {

  def customRun[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Int, ED] = {
    // Transform the edge data something cheap to shuffle and then canonicalize
    val canonicalGraph = graph.mapEdges(e => true).removeSelfEdges().convertToCanonicalEdges()
    // Get the triangle counts
    val counters = TriangleCount.runPreCanonicalized(canonicalGraph).vertices
    // Join them bath with the original graph
    graph.outerJoinVertices(counters) { (vid, _, optCounter: Option[Int]) =>
      optCounter.getOrElse(0)
    }
  }
}