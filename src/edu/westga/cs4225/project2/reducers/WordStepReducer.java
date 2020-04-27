package edu.westga.cs4225.project2.reducers;

import java.io.IOException;

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
public class WordStepReducer extends Reducer<Text, ArrayListWritable<Text>, Text, ArrayListWritable<Text>> {

	@Override
	public void reduce(Text key, Iterable<ArrayListWritable<Text>> values, Context context) throws IOException, InterruptedException {
		ArrayListWritable<Text> writable = new ArrayListWritable<Text>();
		for (ArrayListWritable<Text> current : values) {
			for (Text currentText : current) {
				if (!writable.contains(currentText)) {
					writable.add(currentText);
				}
			}
		}
		context.write(key, writable);
	}
}
