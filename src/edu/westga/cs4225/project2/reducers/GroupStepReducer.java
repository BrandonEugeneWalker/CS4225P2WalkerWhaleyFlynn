package edu.westga.cs4225.project2.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;

/**
 * This is the reducer class.
 * 
 * @author Luke Whaley
 *
 */
public class GroupStepReducer
		extends
		Reducer<ArrayListWritable<Text>, IntWritable, ArrayListWritable<Text>, IntWritable> {

	@Override
	public void reduce(ArrayListWritable<Text> key,
			Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {

		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
