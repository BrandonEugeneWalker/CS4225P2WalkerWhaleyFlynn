package edu.westga.cs4225.project2.main.similarity;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;

public class AggregationStep {

	/**
	 * 
	 * @author Luke Whaley, Brandon Walker, Kevin Flynn 
	 *
	 */
	public static class AggregationStepMapper extends Mapper<Object, Text, IntWritable, ArrayListWritable<Text>> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String[] contents = value.toString().replaceAll("\\[", "").replaceAll("\\]", ",").replaceAll("\\\\s+", "").split(",");
				IntWritable outputKey = new IntWritable(Integer.parseInt(contents[2].trim()));
				ArrayListWritable<Text> outputs = new ArrayListWritable<Text>();
				for (int i = 0; i < contents.length - 1; i++) {
					outputs.add(new Text(contents[i].trim()));
				}
				context.write(outputKey, outputs);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * This is the reducer class.
	 * 
	 * @author Luke Whaley
	 *
	 */
	public static class AggregationStepReducer extends Reducer<IntWritable, ArrayListWritable<Text>, IntWritable, ArrayListWritable<Text>> {

		@Override
		public void reduce(IntWritable key, Iterable<ArrayListWritable<Text>> values, Context context) throws IOException, InterruptedException {
			try {
				ArrayListWritable<Text> aggregation = new ArrayListWritable<Text>();
				for (ArrayListWritable<Text> value : values) {
					for (Text currentText : value) {
						if (!aggregation.contains(currentText)) {
							aggregation.add(currentText);
						}
					}
				}
				context.write(key, aggregation);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
