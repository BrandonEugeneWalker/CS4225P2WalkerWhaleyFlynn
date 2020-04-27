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
 * @author Luke Whaley, Brandon Walker, Kevin Flynn 
 *
 */
public class AggregationStepReducer
		extends
		Reducer<IntWritable, ArrayListWritable<Text>, IntWritable, ArrayListWritable<Text>> {

	@Override
	public void reduce(IntWritable key,
			Iterable<ArrayListWritable<Text>> values, Context context)
			throws IOException, InterruptedException {

		ArrayListWritable<Text> aggregation = new ArrayListWritable<Text>();
		for (ArrayListWritable<Text> value : values) {
			for (Text currentText : value) {
				if (!aggregation.contains(currentText)) {
					aggregation.add(currentText);
				}
			}
		}
		context.write(key, aggregation);

	}
}
