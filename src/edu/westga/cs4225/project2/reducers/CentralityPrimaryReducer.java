package edu.westga.cs4225.project2.reducers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;

/**
 * Reduces the results of CentralityPrimaryMapper.
 * 
 * @author Brandon Walker, Luke Whaley, Kevin Flynn
 *
 */
public class CentralityPrimaryReducer extends
		Reducer<Text, ArrayListWritable<Text>, Text, ArrayListWritable<Text>> {

	/**
	 * Reduces the given input, outputting the abstract, and a list of each
	 * abstract that it shares words with.
	 * 
	 * @precondition none
	 * @postcondition the input is reduced
	 */
	@Override
	public void reduce(Text key, Iterable<ArrayListWritable<Text>> values,
			Context context) throws IOException, InterruptedException {
		ArrayListWritable<Text> group = new ArrayListWritable<Text>();
		for (ArrayListWritable<Text> value : values) {
			for (Text currentText : value) {
				if (!group.contains(currentText)) {
					group.add(currentText);
				}
			}
		}
		context.write(key, group);
	}

}
