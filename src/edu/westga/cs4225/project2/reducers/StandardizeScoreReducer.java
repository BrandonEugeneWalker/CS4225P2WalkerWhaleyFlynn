package edu.westga.cs4225.project2.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * This is the reducer class. It counts all of occurrences of the given kmer.
 * 
 * @author Luke Whaley, Brandon Walker, Kevin Flynn 
 *
 */
public class StandardizeScoreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		for (IntWritable writable : values) {
			context.write(key, writable);
		}

	}
}
