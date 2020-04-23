package edu.westga.cs4225.project2.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;

/**
 * 
 * @author Luke Whaley
 *
 */
public class Similarity {

	/**
	 * 
	 * @author Luke Whaley
	 *
	 */
	public static class MyMapper extends Mapper<Object, Text, Text, ArrayListWritable<Text>> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
		}
	}

	/**
	 * This is the reducer class.
	 * 
	 * @author Luke Whaley
	 *
	 */
	public static class MyReducer extends
		Reducer<Text, ArrayListWritable<Text>, Text, ArrayListWritable<Text>> {

		@Override
		public void reduce(Text key, Iterable<ArrayListWritable<Text>> values, Context context) throws IOException, InterruptedException {
			
		}

	}

	/**
	 * Starting point for the application. Takes 2 arguments. 
	 * Arguments = Input Directory, Output Directory
	 * 
	 * @param args the program arguments.
	 * @throws Exception if any errors occur.
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Similarity <in> <out>");
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(2);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Similarity");
		NLineInputFormat.setNumLinesPerSplit(job, 0);

		job.setJarByClass(Infometrics.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ArrayListWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("similarity-" + args[1] + System.currentTimeMillis()));

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
