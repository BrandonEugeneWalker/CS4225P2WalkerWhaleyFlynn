package edu.westga.cs4225.project2.main.similarity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;

/**
 * 
 * @author Luke Whaley
 *
 */
public class Similarity {

	/**
	 * Starting point for the application. Takes 2 arguments. 
	 * Arguments = Input Directory, Output Directory
	 * 
	 * @param args the program arguments.
	 * @throws Exception if any errors occur.
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Similarity <in> <out>");
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(2);
		}

		Configuration conf = new Configuration();
		Job wordstep = Job.getInstance(conf, "Similarity: Word Step");
		NLineInputFormat.setNumLinesPerSplit(wordstep, 0);
		wordstep.setJarByClass(Similarity.class);
		wordstep.setMapperClass(WordStep.WordStepMapper.class);
		wordstep.setCombinerClass(WordStep.WordStepReducer.class);
		wordstep.setReducerClass(WordStep.WordStepReducer.class);
		wordstep.setOutputKeyClass(Text.class);
		wordstep.setOutputValueClass(ArrayListWritable.class);
		FileInputFormat.addInputPath(wordstep, new Path(args[0]));
		Path outputPath = new Path(args[1] + "/wordstep");
		FileOutputFormat.setOutputPath(wordstep, outputPath);
		wordstep.waitForCompletion(true);
		
		Configuration conf2 = new Configuration();
		Job groupstep = Job.getInstance(conf2, "Similarity: Group Step");
		NLineInputFormat.setNumLinesPerSplit(groupstep, 0);
		groupstep.setJarByClass(Similarity.class);
		groupstep.setMapperClass(GroupStep.GroupStepMapper.class);
		groupstep.setCombinerClass(GroupStep.GroupStepReducer.class);
		groupstep.setReducerClass(GroupStep.GroupStepReducer.class);
		groupstep.setMapOutputKeyClass(ArrayListWritable.class);
		groupstep.setMapOutputValueClass(IntWritable.class);
		groupstep.setOutputKeyClass(ArrayListWritable.class);
		groupstep.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(groupstep, outputPath);
		Path outputPath2 = new Path(args[1] + "/groupstep");
		FileOutputFormat.setOutputPath(groupstep, outputPath2);
		groupstep.waitForCompletion(true);
		
		Configuration conf3 = new Configuration();
		Job aggregationStep = Job.getInstance(conf3, "Similarity: Aggregation Step");
		NLineInputFormat.setNumLinesPerSplit(aggregationStep, 0);
		aggregationStep.setJarByClass(Similarity.class);
		aggregationStep.setMapperClass(AggregationStep.AggregationStepMapper.class);
		aggregationStep.setCombinerClass(AggregationStep.AggregationStepReducer.class);
		aggregationStep.setReducerClass(AggregationStep.AggregationStepReducer.class);
		aggregationStep.setOutputKeyClass(IntWritable.class);
		aggregationStep.setOutputValueClass(ArrayListWritable.class);
		FileInputFormat.addInputPath(aggregationStep, outputPath2);
		FileOutputFormat.setOutputPath(aggregationStep, new Path(args[1] + "/aggregationstep"));
		aggregationStep.waitForCompletion(true);
	}
}
