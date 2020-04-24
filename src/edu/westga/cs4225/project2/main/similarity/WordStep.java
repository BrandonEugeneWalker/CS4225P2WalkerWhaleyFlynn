package edu.westga.cs4225.project2.main.similarity;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;
import edu.westga.cs4225.project2.processing.SimilarityPreprocessor;

public class WordStep {

	/**
	 * 
	 * @author Luke Whaley
	 *
	 */
	public static class WordStepMapper extends Mapper<Object, Text, Text, ArrayListWritable<Text>> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			SimilarityPreprocessor processor = new SimilarityPreprocessor();
			String[] contents = processor.getContents(value.toString());
			ArrayListWritable<Text> writable = new ArrayListWritable<Text>();
			writable.add(new Text(contents[0]));
			for (int i = 1; i < contents.length; i++) {
				String word = contents[i];
				context.write(new Text(word), writable);
			}
		}
	}

	/**
	 * This is the reducer class.
	 * 
	 * @author Luke Whaley
	 *
	 */
	public static class WordStepReducer extends Reducer<Text, ArrayListWritable<Text>, Text, ArrayListWritable<Text>> {

		@Override
		public void reduce(Text key, Iterable<ArrayListWritable<Text>> values, Context context) throws IOException, InterruptedException {
			ArrayListWritable<Text> writable = new ArrayListWritable<Text>();
			for (ArrayListWritable<Text> current : values) {
				writable.addAll(current);
			}
			context.write(key, writable);
		}
	}
	
}
