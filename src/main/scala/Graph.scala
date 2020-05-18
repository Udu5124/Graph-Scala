import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable
import org.apache.spark.rdd.RDD

object Graph 
{
	def main(args: Array[String]) 
	{
		val conf = new SparkConf().setAppName("Connected_Components_Spark_Scala")                       
		val sc = new SparkContext(conf)

		var graph = sc.textFile(args(0)).map(line => {
		val input = line.split(",")                                    
		val adjacent = new Array[Long](input.length - 1)
		var i=1;

		do
		{
           adjacent(i-1) = input(i).toLong
           i=i+1;
        } 
		while(i <=input.length-1); 
		(input(0).toLong, input(0).toLong, adjacent)        })
     
		for (j <- 1 to 5)
		{
			val grp_seq = Flat_Map_Step(graph);
			var min_grp_seq = Find_Min_Group(grp_seq);                            
			var initial_op = graph.map{case(a) => (a._1, a)}
			var joined_seq = min_grp_seq.join(initial_op)                      
			val result = Reconstruct_Graph(joined_seq)                    
			graph = result
		}

		val sort_result = graph.map(graph => (graph._2, 1)).reduceByKey((x, y) => x + y).sortByKey(true, 0) 
		val final_graph =FormatOutput(sort_result)
		final_graph.collect().foreach( println )    
		sc.stop()
	}

	/*	Step 1 : Flat Map	*/
	def Flat_Map_Step(ResultVetrex:RDD[(Long, Long, Array[Long])] ): RDD[(Long,Long)] = 
	{  
        val fm_input = ResultVetrex.flatMap{case(a,b,c) =>
        val grouplen: Int =(c.length) 
        val vertex = new Array[(Long, Long)](grouplen+1)
        vertex(0) = (a, b)
        val adj_vertex: Array[Long] = c
       
        for (index <- 0 to grouplen-1)
        {
			vertex(index + 1) = (adj_vertex(index), b)
        }
        vertex	}
		return fm_input
	}

	/*	Step 2 : Reduce		*/
	def Find_Min_Group(Mingroup:RDD[(Long,Long)]):RDD[(Long,Long)]=
	{
		val min_grp = Mingroup.reduceByKey((a, b) => { 
		var min_group: Long =0

        if (a <= b) 
		{
			min_group = a
        }
        else 
		{
			min_group = b
        }
		min_group	})
		return min_grp
	}

	/*	Step 4 : Reconstruct Graph	*/
	def Reconstruct_Graph(reducedgraph:RDD[(Long, (Long, (Long, Long, Array[Long])))] ): RDD[(Long,Long,Array[Long])]=
	{
        val graph1 = reducedgraph.map{case(a,b) => 
		val adj=b._2
        var conn_vertex =  (a,b._1,adj._3) 	/**VID, group, Vertex **/
		conn_vertex	}
		return graph1
	}
	
	/*	Format Output  */
	def FormatOutput(ResultMatrix:RDD[((Long, Int))]) : RDD[String] = 
	{
		val Reuslt_Matrix_formated = ResultMatrix.map { case ((k,v)) =>k+" "+v}  /*Formating Final Matrix*/
		return Reuslt_Matrix_formated
	}
}