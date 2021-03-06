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

import edu.westga.cs4225.project2.mappers.AbstractCountMapper;
import edu.westga.cs4225.project2.reducers.AbstractCountReducer;

/**
 * Runs the hadoop job for part three.
 * @author Luke Whaley, Brandon Walker, Kevin Flynn 
 *
 */
public class PartThreeAbstractCount {

	/**
	 * Runs the jobs for part 3 of the project.
	 * @param input the input directory
	 * @param output the output directory
	 * @return true if successful
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public static boolean runPartThree(String input, String output) throws IllegalArgumentException, IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Abstract Count");
		job.setJarByClass(PartThreeAbstractCount.class);
		job.setMapperClass(AbstractCountMapper.class);
		job.setCombinerClass(AbstractCountReducer.class);
		job.setReducerClass(AbstractCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		String outputPath = output;
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
