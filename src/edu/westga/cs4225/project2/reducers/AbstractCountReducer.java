package edu.westga.cs4225.project2.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * This is the reducer class. Counts the number of abstracts.
 * 
 * @author Luke Whaley, Brandon Walker, Kevin Flynn 
 *
 */
public class AbstractCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable result = new IntWritable();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		this.result.set(sum);
		context.write(key, this.result);
	}
}
