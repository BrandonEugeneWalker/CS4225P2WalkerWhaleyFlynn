package edu.westga.cs4225.project2.reducers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;

/**
 * Class created to clean data
 * 
 * @author Kevin Flynn Luke Whaley, Brandon Walker
 *
 */
public class DataCleaningReducer extends Reducer<Text, ArrayListWritable<Text>, Text, ArrayListWritable<Text>> {

	@Override
	public void reduce(Text key, Iterable<ArrayListWritable<Text>> values, Context context)
			throws IOException, InterruptedException {
		for (ArrayListWritable<Text> writables : values) {
			context.write(key, writables);
		}
	}
}
