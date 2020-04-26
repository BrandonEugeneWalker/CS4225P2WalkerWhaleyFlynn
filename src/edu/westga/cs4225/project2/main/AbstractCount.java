package edu.westga.cs4225.project2.main;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;
import edu.westga.cs4225.project2.main.Infometrics.MyMapper;
import edu.westga.cs4225.project2.main.Infometrics.MyReducer;
import edu.westga.cs4225.project2.processing.AbstractProcessor;
import edu.westga.cs4225.project2.processing.FileStopwordCollector;

public class AbstractCount {
	private static Path CollectionSizePath = null;
	private static int COLLECTION_SIZE = 0;

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
			String serializedCollector = context.getConfiguration().get("COLLECTOR");
			ObjectInputStream objectinput = new ObjectInputStream(new ByteArrayInputStream(Base64.decodeBase64(serializedCollector)));
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
	
	/**
	 * This is the mapper class that maps all of the data.
	 * The map method counts all of the kmers in the line.
	 * 
	 * @author Kevin Flynn
	 *
	 */
	public static class MyStandardizeMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		
		private static IntWritable ONE = new IntWritable();
		private Text KEY = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				String[] info = value.toString().split("\t");
				double val = (Double.parseDouble(info[1])/ COLLECTION_SIZE ) * 10000;
				ONE.set((int) val);
				Text newKey = new Text(info[0]);
				context.write(newKey, ONE);	
				
			} catch (Exception e) {
				
				System.out.print(value);
			}
	
		}
	}

	/**
	 * This is the reducer class.
	 * It counts all of occurrences of the given kmer.
	 * 
	 * @author Kevin Flynn
	 *
	 */
	public static class MyStandardizeReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
				for(IntWritable writable : values){
					context.write(key, writable);	
				}
			
			

		}
	}
	
	private static void readLine(){
		File file = new File(CollectionSizePath.toString()+ "/part-r-00000");
		try {
		      Scanner myReader = new Scanner(file);
		      while (myReader.hasNextLine()) {
		        String data = myReader.nextLine();
		        String[] allInput = data.split("\t");
		        if(allInput.length == 2){
			        System.out.println(allInput);
			        int value = Integer.parseInt(allInput[1]);
			        COLLECTION_SIZE = value;
		        }

		      }
		      myReader.close();
		    } catch (FileNotFoundException e) {
		      System.out.println("An error occurred.");
		      e.printStackTrace();
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
		if (args.length != 3) {
			System.err.println("Usage: Infometrics <in> <out> <stopword-file>");
			//ToolRunner.printGenericCommandUsage(System.err);
			//System.exit(2);
		}

		FileStopwordCollector collector = new FileStopwordCollector("stopwords.txt");
		ByteArrayOutputStream byteoutput = new ByteArrayOutputStream();
		ObjectOutputStream objectoutput = new ObjectOutputStream(byteoutput);
		
		objectoutput.writeObject(collector);
		objectoutput.close();
		String serializedCollector = new String(Base64.encodeBase64(byteoutput.toByteArray()));
		
		Configuration conf = new Configuration();
		conf.set("COLLECTOR", serializedCollector);
		
		Job job = Job.getInstance(conf, "Abstract Count");
		NLineInputFormat.setNumLinesPerSplit(job, 0);
		
		job.setJarByClass(AbstractCount.class);
		job.setMapperClass(MyCountMapper.class);
		job.setCombinerClass(MyCountReducer.class);
		job.setReducerClass(MyCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path("input/arxiv.txt"));
		CollectionSizePath = new Path("out" + System.currentTimeMillis());
		FileOutputFormat.setOutputPath(job, CollectionSizePath);
		
				
		
		boolean result = job.waitForCompletion(true);
		
		if(result){
			readLine();

			Configuration conf2 = new Configuration();		
			Job job2 = Job.getInstance(conf2, "Standardize count");
			
			NLineInputFormat.setNumLinesPerSplit(job2, 0);
			job2.setJarByClass(AbstractCount.class);
			job2.setMapperClass(MyStandardizeMapper.class);
			job2.setCombinerClass(MyStandardizeReducer.class);
			job2.setReducerClass(MyStandardizeReducer.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job2, new Path("input/results"));
			FileOutputFormat.setOutputPath(job2, new Path("out" + System.currentTimeMillis()));
			boolean result2 = job2.waitForCompletion(true);

			System.exit(result2 ? 0 : 1);

			
		}
		
		
		
		
	}

}
