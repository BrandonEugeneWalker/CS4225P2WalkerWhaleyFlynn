package edu.westga.cs4225.project2.main.similarity;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;
import edu.westga.cs4225.project2.main.Infometrics;
import edu.westga.cs4225.project2.processing.SimilarityPreprocessor;

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
		if (args.length != 3) {
			System.err.println("Usage: Similarity <in> <out> <abstract-words.txt>");
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(2);
		}

		/*SimilarityPreprocessor process = new SimilarityPreprocessor(args[2]);
		HashMap<String, ArrayList<String>> data = process.getData();
		
		ByteArrayOutputStream byteoutput = new ByteArrayOutputStream();
		ObjectOutputStream objectoutput = new ObjectOutputStream(byteoutput);
		objectoutput.writeObject(data);
		objectoutput.close();
		String dataString = new String(Base64.encodeBase64(byteoutput.toByteArray()));*/
		
		
		Configuration conf = new Configuration();
		Job wordstep = Job.getInstance(conf, "Similarity: Word Step");
		NLineInputFormat.setNumLinesPerSplit(wordstep, 0);
		wordstep.setJarByClass(Infometrics.class);
		wordstep.setMapperClass(WordStep.WordStepMapper.class);
		wordstep.setCombinerClass(WordStep.WordStepReducer.class);
		wordstep.setReducerClass(WordStep.WordStepReducer.class);
		wordstep.setOutputKeyClass(Text.class);
		wordstep.setOutputValueClass(ArrayListWritable.class);
		FileInputFormat.addInputPath(wordstep, new Path(args[0]));
		Path outputPath = new Path(args[1] + "/wordstep");
		FileOutputFormat.setOutputPath(wordstep, outputPath);
		
		ControlledJob wordstepControl = new ControlledJob(conf);
		wordstepControl.setJob(wordstep);
		
		Configuration conf2 = new Configuration();
		Job groupstep = Job.getInstance(conf2, "Similarity: Group Step");
		NLineInputFormat.setNumLinesPerSplit(groupstep, 0);
		groupstep.setJarByClass(Infometrics.class);
		groupstep.setMapperClass(GroupStep.GroupStepMapper.class);
		groupstep.setCombinerClass(GroupStep.GroupStepReducer.class);
		groupstep.setReducerClass(GroupStep.GroupStepReducer.class);
		groupstep.setOutputKeyClass(Text.class);
		groupstep.setOutputValueClass(ArrayListWritable.class);
		FileInputFormat.addInputPath(groupstep, outputPath);
		FileOutputFormat.setOutputPath(groupstep, new Path(args[1] + "/groupstep"));
		
		ControlledJob groupstepControl = new ControlledJob(conf2);
		groupstepControl.setJob(groupstep);
		
		JobControl control = new JobControl("Controller");
		control.addJob(wordstepControl);
		control.addJob(groupstepControl);
		groupstepControl.addDependingJob(wordstepControl);
		
		control.run();
	}
}
