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
import edu.westga.cs4225.project2.processing.AbstractProcessor;
import edu.westga.cs4225.project2.processing.FileStopwordCollector;

/**
 * 
 * @author cloudera
 *
 */
public class PartTwoInfometrics {
	
	
	public static class MyMapper extends
			Mapper<Object, Text, Text, ArrayListWritable<Text>> {
		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String serializedCollector = context.getConfiguration().get("COLLECTOR");
			ObjectInputStream objectinput = new ObjectInputStream(new ByteArrayInputStream(Base64.decodeBase64(serializedCollector)));
			try {
				FileStopwordCollector collector = (FileStopwordCollector) objectinput.readObject();
				AbstractProcessor processor = new AbstractProcessor(collector);
				processor.process(value.toString());
				context.write(processor.getKey(), processor.getValue());
			} catch (Exception e) {
				e.printStackTrace();
				throw new IOException();
			} finally {
				objectinput.close();
			}
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
			Reducer<Text, ArrayListWritable<Text>, Text, ArrayListWritable<Text>> {
		
		@Override
		public void reduce(Text key, Iterable<ArrayListWritable<Text>> values,
				Context context) throws IOException, InterruptedException {
			for (ArrayListWritable<Text> writables : values) {
				context.write(key, writables);
			}
		}
	}
	
	public static boolean runPartTwo(String input, String output, String stopwordFile) throws IOException, ClassNotFoundException, InterruptedException {
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
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ArrayListWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true);
	}
	
}
