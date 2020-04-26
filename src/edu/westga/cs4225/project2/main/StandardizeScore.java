package edu.westga.cs4225.project2.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StandardizeScore {
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
				int size = Integer.parseInt(context.getConfiguration().get("Collection_Size"));
				
				
				double val = (Double.parseDouble(info[1])/ (size - 1) ) * 10000;
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
	
	
	/**
	 * This is the mapper class that maps all of the data.
	 * The map method counts all of the kmers in the line.
	 * 
	 * @author Kevin Flynn
	 *
	 */
	public static class MySizeMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		
		private static IntWritable ONE = new IntWritable();
		private Text KEY = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			try {				
		        String[] allInput = value.toString().split("\t");
		        if (allInput.length == 2){
			        System.out.println(allInput);
			        int val = Integer.parseInt(allInput[1]);
			        context.getConfiguration().set(allInput[0], val + ""); 
		        }
				
			} catch (Exception e) {
				
				System.out.print(value);
			}
	
		}
	}

	
	
	
	private static void readCollectionSize(Path collectionSizePath, Configuration conf){
		File file = new File(collectionSizePath.toString());
		try {
		      Scanner myReader = new Scanner(file);
		      while (myReader.hasNextLine()) {
		        String data = myReader.nextLine();
		        String[] allInput = data.split("\t");
		        if (allInput.length == 2){
			        System.out.println(allInput);
			        int value = Integer.parseInt(allInput[1]);
			        conf.set(allInput[0], value + ""); 
			        
		        }

		      }
		      myReader.close();
		    } catch (FileNotFoundException e) {
		      System.out.println("An error occurred.");
		      e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		boolean result2 = false;
		
		Configuration conf2 = new Configuration();
		//readCollectionSize(new Path(args[0]),conf2);
		
		Job job3 = Job.getInstance(conf2, "Get Size");
		job3.setJarByClass(StandardizeScore.class);
		job3.setMapperClass(MySizeMapper.class);
		FileInputFormat.addInputPath(job3, new Path(args[0]));
		FileOutputFormat.setOutputPath(job3, new Path(args[2] + "/part5/" +"ignore" + System.currentTimeMillis()));

		boolean result3 = false;
		try {
			result3 = job3.waitForCompletion(true);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		
		
		Job job2 = Job.getInstance(conf2, "Standardize count");
		
		NLineInputFormat.setNumLinesPerSplit(job2, 0);
		job2.setJarByClass(StandardizeScore.class);
		job2.setMapperClass(MyStandardizeMapper.class);
		job2.setCombinerClass(MyStandardizeReducer.class);
		job2.setReducerClass(MyStandardizeReducer.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		//results
		FileInputFormat.addInputPath(job2, new Path(args[1]));

		FileOutputFormat.setOutputPath(job2, new Path(args[2] + "/part5/" +"out" + System.currentTimeMillis()));
		try {
			result2 = job2.waitForCompletion(true);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		System.exit((result2 && result3)  ? 0 : 1);		

	
	}
	
}
