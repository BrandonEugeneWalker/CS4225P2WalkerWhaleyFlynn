package edu.westga.cs4225.project2.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;
import edu.westga.cs4225.project2.processing.SimilarityPreprocessor;
import edu.westga.cs4225.project2.reducers.CentralityPrimaryReducer;
import edu.westga.cs4225.project2.reducers.CentralitySecondaryReducer;
import edu.westga.cs4225.project2.main.similarity.WordStep;
import edu.westga.cs4225.project2.mappers.CentralityPrimaryMapper;
import edu.westga.cs4225.project2.mappers.CentralitySecondaryMapper;

/**
 * Determines the DegreeCentrality of each abstract.
 * 
 * @author Brandon Walker, Luke Whaley, Kevin flynn
 *
 */
public class PartFourDegreeCentralityFinder {

	public static boolean runPartFour(String input, String output) throws IllegalArgumentException, IOException {
		Configuration conf = new Configuration();
		String outputFileName0 = output + "/part0";
		String outputFileName1 = output + "/part1";
		String outputFileName2 = output + "/part2";

		Job job0 = Job.getInstance(conf, "degree centrality finder word mapping");
		job0.setJarByClass(PartFourDegreeCentralityFinder.class);
		job0.setMapperClass(WordStep.WordStepMapper.class);
		job0.setCombinerClass(WordStep.WordStepReducer.class);
		job0.setReducerClass(WordStep.WordStepReducer.class);
		job0.setMapOutputKeyClass(Text.class);
		job0.setMapOutputValueClass(ArrayListWritable.class);
		job0.setOutputKeyClass(Text.class);
		job0.setOutputKeyClass(ArrayListWritable.class);
		FileInputFormat.addInputPath(job0, new Path(input));
		FileOutputFormat.setOutputPath(job0, new Path(outputFileName0));

		boolean results0 = false;
		try {
			results0 = job0.waitForCompletion(true);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}

		Job job1 = Job.getInstance(conf, "degree centrality finder first half");
		job1.setJarByClass(PartFourDegreeCentralityFinder.class);
		job1.setMapperClass(CentralityPrimaryMapper.class);
		job1.setCombinerClass(CentralityPrimaryReducer.class);
		job1.setReducerClass(CentralityPrimaryReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(ArrayListWritable.class);
		FileInputFormat.addInputPath(job1, new Path(outputFileName0));
		FileOutputFormat.setOutputPath(job1, new Path(outputFileName1));

		boolean results1 = false;
		try {
			results1 = job1.waitForCompletion(true);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}

		Job job2 = Job.getInstance(conf, "degree centrality finder second half");
		job2.setJarByClass(PartFourDegreeCentralityFinder.class);
		job2.setMapperClass(CentralitySecondaryMapper.class);
		job2.setCombinerClass(CentralitySecondaryReducer.class);
		job2.setReducerClass(CentralitySecondaryReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(outputFileName1));
		FileOutputFormat.setOutputPath(job2, new Path(outputFileName2));

		boolean results2 = false;
		try {
			results2 = job2.waitForCompletion(true);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}

		return results0 && results1 && results2;
	}

}
