package edu.westga.cs4225.project2.mappers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * This is the mapper class that maps all of the data. The map method counts all
 * of the kmers in the line.
 * 
 * @author Kevin Flynn
 *
 */
public class StandardizeScoreMapper extends Mapper<Object, Text, Text, IntWritable> {

	private static IntWritable one = new IntWritable();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] info = value.toString().split("\t");
		Configuration conf = context.getConfiguration();
		int size = conf.getInt("number", -1);

		double val = (Double.parseDouble(info[1]) / (size - 1)) * 10000;
		one.set((int) val);
		Text newKey = new Text(info[0]);
		context.write(newKey, one);

	}
}
