package edu.westga.cs4225.project2.main;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PartFiveStandardizedScore {
	
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

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			try {				
				String[] info = value.toString().split("\t");
				Configuration conf = context.getConfiguration();
				int size = conf.getInt("number", -1);
				
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
	
	public static boolean runPartFive(String partThreeOutputDirectory, String partFourPartTwoOutputDirectory, String output) throws IllegalArgumentException, IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path in = new Path(partThreeOutputDirectory);
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(in, true);
		while (fileStatusListIterator.hasNext()) {
			LocatedFileStatus fileStatus = fileStatusListIterator.next();
			FSDataInputStream stream = fs.open(fileStatus.getPath());
			Integer count = null;
			try (Scanner scan = new Scanner(stream.getWrappedStream())) {
				if (scan.hasNextLine()) {
					String line = scan.nextLine();
					String[] contents = line.split("\\s+");
					String number = contents[1];
					count = Integer.parseInt(number);
					conf.setInt("number", count);
					break;
				}
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		}
		Job job2 = Job.getInstance(conf, "Standardize count");
		job2.setJarByClass(PartFiveStandardizedScore.class);
		job2.setMapperClass(MyStandardizeMapper.class);
		job2.setCombinerClass(MyStandardizeReducer.class);
		job2.setReducerClass(MyStandardizeReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(partFourPartTwoOutputDirectory));
		FileOutputFormat.setOutputPath(job2, new Path(output + "/part5/" +"out" + System.currentTimeMillis()));
		
		boolean result = false;
		try {
			result = job2.waitForCompletion(true);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		return result;
	}
	
}
