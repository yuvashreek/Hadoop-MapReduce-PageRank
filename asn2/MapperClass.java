package cps534.asn2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Object, Text, Object, Text> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Node inNode = new Node(value.toString()); // input node
		Double p = inNode.getPageRank() / inNode.getAdjList().size();
		// context.write(new Text(inNode.getId()), new Text(inNode.getNodeInfo())); //
		// pass along the graph
		context.write(new Text(inNode.getId()), new Text(value));
		for (String adjList : inNode.getAdjList()) {
			Node adjNode = new Node(p.toString());
			context.write(new Text(adjList), new Text(Double.toString(adjNode.getPageRank())));
		}
	}
}