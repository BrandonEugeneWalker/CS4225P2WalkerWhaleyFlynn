package edu.westga.cs4225.project2.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;
import edu.westga.cs4225.project2.processing.SimilarityPreprocessor;

/**
 * 
 * @author Luke Whaley
 *
 */
public class GroupStepMapper extends
		Mapper<Object, Text, ArrayListWritable<Text>, IntWritable> {

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
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

	}
}
