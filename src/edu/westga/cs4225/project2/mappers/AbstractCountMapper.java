package edu.westga.cs4225.project2.mappers;

import java.io.IOException;

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
public class AbstractCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	private static IntWritable ONE = new IntWritable(1);

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Text Collection_Size = new Text("Collection_Size");
		context.write(Collection_Size, ONE);

	}
}
