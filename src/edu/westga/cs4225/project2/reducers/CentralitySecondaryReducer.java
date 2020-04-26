package edu.westga.cs4225.project2.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


/**
 * Reduces the mapping of CentralitySecondaryMapper into the final desired results for part 4.
 * 
 * @author Brandon Walker, Luke Whaley, Kevin Flynn
 *
 */
public class CentralitySecondaryReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable result = new IntWritable();

	/**
	 * Reduces the input by simply adding up the total count for each key value.
	 * This is basically the reducer from the word count and kmer count.
	 * 
	 * @precondition none
	 * @postcondition the input is reduced
	 */
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}
