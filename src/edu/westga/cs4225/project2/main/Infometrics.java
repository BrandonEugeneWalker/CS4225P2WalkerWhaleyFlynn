package edu.westga.cs4225.project2.main;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import edu.westga.cs4225.project2.input.MyInputFormatNLines;

/**
 * This class contains the Mapper and Reducer classes as well
 * as the starting point for the application.
 * 
 * @author Luke Whaley, Brandon Walker, Kevin Flynn
 *
 */
public class Infometrics {
	
	/**
	 * This is the mapper class that maps all of the data.
	 * The map method counts all of the kmers in the line.
	 * 
	 * @author Luke Whaley
	 *
	 */
	public static class MyMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		
		private static final IntWritable ONE = new IntWritable(1);
		private Text word = new Text();
		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
		}
	}

	/**
	 * This is the reducer class.
	 * It counts all of occurrences of the given kmer.
	 * 
	 * @author Luke Whaley
	 *
	 */
	public static class MyReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			
		}
	}
	
	/**
	 * Starting point for the application. Takes 3 arguments. 
	 * Arguments = Input Directory, Output Directory, K-Count.
	 * 
	 * @param args the program arguments.
	 * @throws Exception if any errors occur.
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Infometrics <in> <out>");
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(2);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Infometrics");
		NLineInputFormat.setNumLinesPerSplit(job, 0);
		
		job.setJarByClass(Infometrics.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);

		job.setInputFormatClass(MyInputFormatNLines.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + System.currentTimeMillis()));
		
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
