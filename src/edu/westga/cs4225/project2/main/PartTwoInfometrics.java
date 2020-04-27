package edu.westga.cs4225.project2.main;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;
import edu.westga.cs4225.project2.mappers.DataCleaningMapper;
import edu.westga.cs4225.project2.processing.AbstractProcessor;
import edu.westga.cs4225.project2.processing.FileStopwordCollector;
import edu.westga.cs4225.project2.reducers.DataCleaningReducer;

/**
 * Runs the hadoop jobs for part two.
 * @author Luke Whaley, Brandon Walker, Kevin Flynn 
 *
 */
public class PartTwoInfometrics {

	/**
	 * Runs the jobs for part 2 of the project.
	 * @param input the input directory
	 * @param output the output directory
	 * @param stopwordFile the stop-word file location
	 * @return true if successful
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static boolean runPartTwo(String input, String output, String stopwordFile)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream stream = fs.open(new Path(stopwordFile));

		FileStopwordCollector collector = new FileStopwordCollector(stream.getWrappedStream());
		ByteArrayOutputStream byteoutput = new ByteArrayOutputStream();
		ObjectOutputStream objectoutput = new ObjectOutputStream(byteoutput);

		objectoutput.writeObject(collector);
		objectoutput.close();
		String serializedCollector = new String(Base64.encodeBase64(byteoutput.toByteArray()));

		conf.set("COLLECTOR", serializedCollector);
		Job job = Job.getInstance(conf, "Infometrics");
		job.setJarByClass(PartTwoInfometrics.class);
		job.setMapperClass(DataCleaningMapper.class);
		job.setCombinerClass(DataCleaningReducer.class);
		job.setReducerClass(DataCleaningReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ArrayListWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job.waitForCompletion(true);
	}

}
