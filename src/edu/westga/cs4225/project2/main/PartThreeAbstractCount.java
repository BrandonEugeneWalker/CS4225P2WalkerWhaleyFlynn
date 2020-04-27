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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author Kevin Flynn
 *
 */
public class PartThreeAbstractCount {

	/**
	 * This is the mapper class that maps all of the data.
	 * The map method counts all of the kmers in the line.
	 * 
	 * @author Kevin Flynn
	 *
	 */
	public static class MyCountMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		
		private static IntWritable ONE = new IntWritable(1);

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Text Collection_Size = new Text("Collection_Size");
			context.write(Collection_Size, ONE);
			
		}
	}

	/**
	 * This is the reducer class.
	 * It counts all of occurrences of the given kmer.
	 * 
	 * @author Kevin Flynn
	 *
	 */
	public static class MyCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}	
			result.set(sum);
			context.write(key, result);	
		}
	}
	
	public static boolean runPartThree(String input, String output) throws IllegalArgumentException, IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Abstract Count");
		job.setJarByClass(PartThreeAbstractCount.class);
		job.setMapperClass(MyCountMapper.class);
		job.setCombinerClass(MyCountReducer.class);
		job.setReducerClass(MyCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		String outputPath = output + "/part3/out";
		Path collectionSizePath = new Path(outputPath);
		FileOutputFormat.setOutputPath(job, collectionSizePath);	
		
		boolean result = false;
		try {
			result = job.waitForCompletion(true);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		return result;
	}

}
