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
import edu.westga.cs4225.project2.main.similarity.WordStep;

/**
 * Determines the DegreeCentrality of each abstract.
 * 
 * @author Brandon Walker, Luke Whaley, Kevin flynn
 *
 */
public class DegreeCentralityFinder {

	/**
	 * Maps each abstract with each other.
	 * 
	 * @author Brandon Walker, Luke Whaley, Kevin Flynn
	 *
	 */
	public static class CentralityPrimaryMapper extends Mapper<Object, Text, Text, ArrayListWritable<Text>> {

		/**
		 * Maps each abstract with the list of its words.
		 * 
		 * @precondition none
		 * @postcondition the input data was mapped
		 */
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				SimilarityPreprocessor processor = new SimilarityPreprocessor();
				String[] contents = processor.getContents(value.toString());
				if (contents.length > 1) {
					for (int i = 1; i < contents.length - 1; i++) {
						Text currentI = new Text(contents[i]);
						ArrayListWritable<Text> group = new ArrayListWritable<Text>();
						for (int j = 1; j < contents.length; j++) {
							Text currentJ = new Text(contents[j]);
							if (i != j && !group.contains(currentJ)) {
								group.add(currentJ);
							}
						}
						context.write(currentI, group);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Reduces the results of CentralityPrimaryMapper.
	 * 
	 * @author Brandon Walker, Luke Whaley, Kevin Flynn
	 *
	 */
	public static class CentralityPrimaryReducer extends Reducer<Text, ArrayListWritable<Text>, Text, ArrayListWritable<Text>> {

		/**
		 * Reduces the given input, outputting the abstract, and a list of each
		 * abstract that it shares words with.
		 * 
		 * @precondition none
		 * @postcondition the input is reduced
		 */
		@Override
		public void reduce(Text key, Iterable<ArrayListWritable<Text>> values, Context context) throws IOException, InterruptedException {
			ArrayListWritable<Text> group = new ArrayListWritable<Text>();
			for (ArrayListWritable<Text> value : values) {
				for (Text currentText : value) {
					if (!group.contains(currentText)) {
						group.add(currentText);
					}
				}
			}
			context.write(key, group);
		}

	}

	/**
	 * Maps on the results of the first map/reduce job.
	 * 
	 * @author Brandon Walker, Luke Whaley, Kevin Flynn
	 *
	 */
	public static class CentralitySecondaryMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);

		/**
		 * Using the results from the first job, it maps each number of another
		 * abstract attached to the key abstract.
		 * 
		 * @precondition none
		 * @postcondition the input is mapped
		 */
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String replacedCommad = line.replace(",", "");
			String replaceLeftBracket = replacedCommad.replace("[", "");
			String replaceRightBracket = replaceLeftBracket.replace("]", "");
			String replaceTab = replaceRightBracket.replace("\t", " ");
			String[] splitLine = replaceTab.split(" ");
			String keyAbstract = splitLine[0];
			Text keyAbstractText = new Text(keyAbstract);

			for (int i = 1; i < splitLine.length; i++) {
				context.write(keyAbstractText, ONE);
			}
		}
	}

	/**
	 * Reduces the mapping of CentralitySecondaryMapper.
	 * 
	 * @author Brandon Walker, Luke Whaley, Kevin Flynn
	 *
	 */
	public static class CentralitySecondaryReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		/**
		 * Reduces the input by simply adding up the total count for each key
		 * value. This is basically the reducer from the word count and kmer
		 * count.
		 * 
		 * @precondition none
		 * @postcondition the input is reduced
		 */
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	/**
	 * The entry point to the application.
	 * 
	 * @param args
	 *            the command line arguments
	 * @throws Exception
	 *             if anything bad happens
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: DegreeCentralityFinder <in> <out>");
			System.exit(2);
		}

		String outputFileName0 = args[1] + "/part0";
		String outputFileName1 = args[1] + "/part1";
		String outputFileName2 = args[1] + "/part2";
		
		Job job0 = Job.getInstance(conf, "degree centrality finder word mapping");
		job0.setJarByClass(DegreeCentralityFinder.class);
		job0.setMapperClass(WordStep.WordStepMapper.class);
		job0.setCombinerClass(WordStep.WordStepReducer.class);
		job0.setReducerClass(WordStep.WordStepReducer.class);
		job0.setMapOutputKeyClass(Text.class);
		job0.setMapOutputValueClass(ArrayListWritable.class);
		job0.setOutputKeyClass(Text.class);
		job0.setOutputKeyClass(ArrayListWritable.class);
		FileInputFormat.addInputPath(job0, new Path(args[0]));
		FileOutputFormat.setOutputPath(job0, new Path(outputFileName0));
		
		boolean results0 = false;
		try {
			results0 = job0.waitForCompletion(true);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		
		Job job1 = Job.getInstance(conf, "degree centrality finder first half");
		job1.setJarByClass(DegreeCentralityFinder.class);
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
		job2.setJarByClass(DegreeCentralityFinder.class);
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

		System.exit((results0 && results1 && results2) ? 0 : 1);

	}

}
