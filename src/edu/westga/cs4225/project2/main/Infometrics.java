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
import org.apache.hadoop.util.ToolRunner;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;
import edu.westga.cs4225.project2.processing.AbstractProcessor;
import edu.westga.cs4225.project2.processing.FileStopwordCollector;

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
	
	/**
	 * Starting point for the application. Takes 3 arguments. 
	 * Arguments = Input Directory, Output Directory, Stopword file path.
	 * 
	 * @param args the program arguments.
	 * @throws Exception if any errors occur.
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: Infometrics <in> <out> <stopword-file>");
			System.exit(2);
		}
		
		String partTwoInputLocation = args[0];
		String initialOutputLocation = args[1] + System.currentTimeMillis();
		String stopwordInputLocation = args[2];
		
		String partTwoOutputLocation = initialOutputLocation + "/part2";
		
		boolean partTwoResults = PartTwoInfometrics.runPartTwo(partTwoInputLocation, partTwoOutputLocation, stopwordInputLocation);
		
		
	}
}
