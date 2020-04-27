package edu.westga.cs4225.project2.main;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.westga.cs4225.project2.mappers.StandardizeScoreMapper;
import edu.westga.cs4225.project2.reducers.StandardizeScoreReducer;

public class PartFiveStandardizedScore {

	public static boolean runPartFive(String partThreeInputFile, String partFourInputDirectory, String output)
			throws IllegalArgumentException, IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path in = new Path(partThreeInputFile);
		if (!fs.exists(in)) {
			System.exit(-1);
		}

		FSDataInputStream stream = fs.open(in);
		Integer count = null;
		try (Scanner scan = new Scanner(stream.getWrappedStream())) {
			String line = scan.nextLine();
			String[] contents = line.split("\\s+");
			String number = contents[1];
			count = Integer.parseInt(number);
		}

		conf.setInt("number", count);
		Job job2 = Job.getInstance(conf, "Standardize count");

		job2.setJarByClass(PartFiveStandardizedScore.class);
		job2.setMapperClass(StandardizeScoreMapper.class);
		job2.setCombinerClass(StandardizeScoreReducer.class);
		job2.setReducerClass(StandardizeScoreReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(partFourInputDirectory));
		FileOutputFormat.setOutputPath(job2, new Path(output + "/part5/" + "out" + System.currentTimeMillis()));

		boolean result = false;
		try {
			result = job2.waitForCompletion(true);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		return result;
	}

}
