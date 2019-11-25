package cps534.asn2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, Text, Text, Text> {

	public static enum Counter { // group name
		DELTAS // count name
	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Double num = 0.0;
		Node resultNode = null;
		// multiple iteration
		StringBuilder oldPageRank = new StringBuilder();
		final int CONVERGENCE_SCALING_FACTOR = 1000;
		//
		for (Text text : values) {
			Node n = new Node(text.toString());
			if (n.isNode()) {
				oldPageRank.append(n.getPageRank());
				resultNode = n;
			} else {
				num += n.getPageRank();
			}
		}
		// multiple iterations
		double delta = Double.parseDouble(oldPageRank.toString()) - num;
		int scaledDelta = Math.abs((int) (delta * CONVERGENCE_SCALING_FACTOR));
		// e.g., CONVERGENCE_SCALING_FACTOR is 1000
		context.getCounter(Counter.DELTAS).increment(scaledDelta);
		//
		resultNode.setPageRank(num);
		StringBuilder s = new StringBuilder();
		for (int i = 0; i < resultNode.getAdjList().size(); i++) {
			if (i != resultNode.getAdjList().size() - 1) {
				s.append(resultNode.getAdjList().get(i)).append(",");
			} else {
				s.append(resultNode.getAdjList().get(i));
			}

		}
		context.write(new Text(key), new Text(resultNode.getPageRank() + "\t" + s));
	}

}