package edu.westga.cs4225.project2.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;

/**
 * This mapper class is used to map and store values for aggregation
 * 
 * @author Luke Whaley
 *
 */
public class AggregationStepMapper extends
		Mapper<Object, Text, IntWritable, ArrayListWritable<Text>> {

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] contents = value.toString().replaceAll("\\[", "")
				.replaceAll("\\]", ",").replaceAll("\\\\s+", "").split(",");
		IntWritable outputKey = new IntWritable(Integer.parseInt(contents[2]
				.trim()));
		ArrayListWritable<Text> outputs = new ArrayListWritable<Text>();
		for (int i = 0; i < contents.length - 1; i++) {
			outputs.add(new Text(contents[i].trim()));
		}
		context.write(outputKey, outputs);

	}
}
