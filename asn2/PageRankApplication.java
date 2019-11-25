package cps534.asn2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRankApplication extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PageRankApplication(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("usage: [input] [output]");
			System.exit(-1);
		}
		long desiredConvergence = 10;
		String jobName = "pgjob";
		int iterCount = 0;
		String input = "", output = "";
		while (true) {
			Job job = Job.getInstance(new Configuration(), jobName);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapperClass(MapperClass.class);
			job.setReducerClass(ReducerClass.class);
			job.setNumReduceTasks(1);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setJarByClass(PageRankApplication.class);

			if (iterCount == 0) {
				input = args[0];
			} else {
				input = args[1] + iterCount;
			}

			output = args[1] + (iterCount + 1);
			
			//iterCount++;

			FileInputFormat.setInputPaths(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));

			//job.submit();
			job.waitForCompletion(true);
			long summedConvergence = job.getCounters().findCounter(ReducerClass.Counter.DELTAS).getValue();
			if (summedConvergence < desiredConvergence) { // designedConvergence is fixed
				break; // done with all iterations
			}
			iterCount++;
		}

		return 0;
	}
}
