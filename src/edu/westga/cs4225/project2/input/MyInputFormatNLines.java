package edu.westga.cs4225.project2.input;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

/**
 * Custom input format that returns the custom 
 * record reader.
 * 
 * @author Luke Whaley, Brandon Walker, Kevin Flynn
 *
 */
public class MyInputFormatNLines extends NLineInputFormat {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {
		return new MyNLineRecordReader();
	}

}
