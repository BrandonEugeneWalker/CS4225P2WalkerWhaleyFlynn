package edu.westga.cs4225.project2.mappers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;
import edu.westga.cs4225.project2.processing.SimilarityPreprocessor;

/**
 * Class created to map keys and values for word step
 * 
 * @author Luke Whaley
 *
 */
public class WordStepMapper extends Mapper<Object, Text, Text, ArrayListWritable<Text>> {

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
