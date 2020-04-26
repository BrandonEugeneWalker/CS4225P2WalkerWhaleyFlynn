package edu.westga.cs4225.project2.main.similarity;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;
import edu.westga.cs4225.project2.processing.SimilarityPreprocessor;

public class GroupStep {

	/**
	 * 
	 * @author Luke Whaley
	 *
	 */
	public static class GroupStepMapper extends Mapper<Object, Text, ArrayListWritable<Text>, IntWritable> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				SimilarityPreprocessor processor = new SimilarityPreprocessor();
				String[] contents = processor.getContents(value.toString());
				if (contents.length > 1) {
					for (int i = 1; i < contents.length - 1; i++) {
						String currentI = contents[i];
						for (int j = i + 1; j < contents.length; j++) {
							String currentJ = contents[j];
							ArrayListWritable<Text> group = new ArrayListWritable<Text>();
							group.add(new Text(currentI));
							group.add(new Text(currentJ));
							context.write(group, new IntWritable(1));
						}
					}
				}
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
	public static class GroupStepReducer extends Reducer<ArrayListWritable<Text>, IntWritable, ArrayListWritable<Text>, IntWritable> {

		@Override
		public void reduce(ArrayListWritable<Text> key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			try {
				int sum = 0;
				for (IntWritable value : values) {
					sum += value.get();
				}
				context.write(key, new IntWritable(sum));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
}
