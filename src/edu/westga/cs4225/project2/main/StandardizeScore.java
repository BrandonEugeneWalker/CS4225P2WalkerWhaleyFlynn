package edu.westga.cs4225.project2.main;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

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
				double val = (Double.parseDouble(info[1])/ (AbstractSize.COLLECTION_SIZE - 1) ) * 10000;
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
	
	
}
