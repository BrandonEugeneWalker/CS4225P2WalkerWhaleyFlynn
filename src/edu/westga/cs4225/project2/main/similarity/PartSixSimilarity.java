package edu.westga.cs4225.project2.main.similarity;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;

/**
 * 
 * @author Luke Whaley
 *
 */
public class PartSixSimilarity {

	/**
	 * 
	 * @param input
	 * @param output
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static boolean runPartSix(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job wordstep = Job.getInstance(conf, "Similarity: Word Step");
		NLineInputFormat.setNumLinesPerSplit(wordstep, 0);
		wordstep.setJarByClass(PartSixSimilarity.class);
		wordstep.setMapperClass(WordStep.WordStepMapper.class);
		wordstep.setCombinerClass(WordStep.WordStepReducer.class);
		wordstep.setReducerClass(WordStep.WordStepReducer.class);
		wordstep.setOutputKeyClass(Text.class);
		wordstep.setOutputValueClass(ArrayListWritable.class);
		FileInputFormat.addInputPath(wordstep, new Path(input));
		Path outputPath = new Path(output + "/wordstep");
		FileOutputFormat.setOutputPath(wordstep, outputPath);
		wordstep.waitForCompletion(true);
		
		Configuration conf2 = new Configuration();
		Job groupstep = Job.getInstance(conf2, "Similarity: Group Step");
		NLineInputFormat.setNumLinesPerSplit(groupstep, 0);
		groupstep.setJarByClass(PartSixSimilarity.class);
		groupstep.setMapperClass(GroupStep.GroupStepMapper.class);
		groupstep.setCombinerClass(GroupStep.GroupStepReducer.class);
		groupstep.setReducerClass(GroupStep.GroupStepReducer.class);
		groupstep.setMapOutputKeyClass(ArrayListWritable.class);
		groupstep.setMapOutputValueClass(IntWritable.class);
		groupstep.setOutputKeyClass(ArrayListWritable.class);
		groupstep.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(groupstep, outputPath);
		Path outputPath2 = new Path(output + "/groupstep");
		FileOutputFormat.setOutputPath(groupstep, outputPath2);
		groupstep.waitForCompletion(true);
		
		Configuration conf3 = new Configuration();
		Job aggregationStep = Job.getInstance(conf3, "Similarity: Aggregation Step");
		NLineInputFormat.setNumLinesPerSplit(aggregationStep, 0);
		aggregationStep.setJarByClass(PartSixSimilarity.class);
		aggregationStep.setMapperClass(AggregationStep.AggregationStepMapper.class);
		aggregationStep.setCombinerClass(AggregationStep.AggregationStepReducer.class);
		aggregationStep.setReducerClass(AggregationStep.AggregationStepReducer.class);
		aggregationStep.setOutputKeyClass(IntWritable.class);
		aggregationStep.setOutputValueClass(ArrayListWritable.class);
		FileInputFormat.addInputPath(aggregationStep, outputPath2);
		FileOutputFormat.setOutputPath(aggregationStep, new Path(output + "/aggregationstep"));
		aggregationStep.waitForCompletion(true);
		
		return wordstep.isSuccessful() && groupstep.isSuccessful() && aggregationStep.isSuccessful();
		
	}
	
}
