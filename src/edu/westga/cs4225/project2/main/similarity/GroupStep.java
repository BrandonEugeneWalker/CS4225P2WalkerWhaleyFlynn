package edu.westga.cs4225.project2.main.similarity;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;
import edu.westga.cs4225.project2.processing.SimilarityPreprocessor;

public class GroupStep {

	/**
	 * 
	 * @author Luke Whaley
	 *
	 */
	public static class GroupStepMapper extends Mapper<Object, Text, Text, ArrayListWritable<Text>> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				SimilarityPreprocessor processor = new SimilarityPreprocessor();
				String[] contents = processor.getContents(value.toString());
				for (int i = 1; i < contents.length; i++) {
					String currentKey = contents[i];
					ArrayListWritable<Text> currentGroup = new ArrayListWritable<Text>();
					for (int j = 1; j < contents.length; j++) {
						if (j != i) {
							currentGroup.add(new Text(contents[j]));
						}
					}
					context.write(new Text(currentKey), currentGroup);
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
	public static class GroupStepReducer extends Reducer<Text, ArrayListWritable<Text>, Text, ArrayListWritable<Text>> {

		@Override
		public void reduce(Text key, Iterable<ArrayListWritable<Text>> values, Context context) throws IOException, InterruptedException {
			try {
				ArrayListWritable<Text> words = new ArrayListWritable<Text>();
				for (ArrayListWritable<Text> value : values) {
					words.addAll(value);
				}
				context.write(key, words);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
}
