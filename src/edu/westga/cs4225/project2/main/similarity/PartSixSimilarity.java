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
import edu.westga.cs4225.project2.mappers.AggregationStepMapper;
import edu.westga.cs4225.project2.mappers.GroupStepMapper;
import edu.westga.cs4225.project2.mappers.WordStepMapper;
import edu.westga.cs4225.project2.reducers.AggregationStepReducer;
import edu.westga.cs4225.project2.reducers.GroupStepReducer;
import edu.westga.cs4225.project2.reducers.WordStepReducer;

/**
 * 
 * @author Luke Whaley, Brandon Walker, Kevin Flynn 
 *
 */
public class PartSixSimilarity {

	/**
	 * Runs jobs relating to part 6 of the project.
	 * @param input the input directory
	 * @param output the output directory
	 * @return true if successful
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static boolean runPartSix(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job wordstep = Job.getInstance(conf, "Similarity: Word Step");
		NLineInputFormat.setNumLinesPerSplit(wordstep, 0);
		wordstep.setJarByClass(PartSixSimilarity.class);
		wordstep.setMapperClass(WordStepMapper.class);
		wordstep.setCombinerClass(WordStepReducer.class);
		wordstep.setReducerClass(WordStepReducer.class);
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
		groupstep.setMapperClass(GroupStepMapper.class);
		groupstep.setCombinerClass(GroupStepReducer.class);
		groupstep.setReducerClass(GroupStepReducer.class);
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
		aggregationStep.setMapperClass(AggregationStepMapper.class);
		aggregationStep.setCombinerClass(AggregationStepReducer.class);
		aggregationStep.setReducerClass(AggregationStepReducer.class);
		aggregationStep.setOutputKeyClass(IntWritable.class);
		aggregationStep.setOutputValueClass(ArrayListWritable.class);
		FileInputFormat.addInputPath(aggregationStep, outputPath2);
		FileOutputFormat.setOutputPath(aggregationStep, new Path(output + "/aggregationstep"));
		aggregationStep.waitForCompletion(true);
		
		return wordstep.isSuccessful() && groupstep.isSuccessful() && aggregationStep.isSuccessful();
		
	}
	
}
