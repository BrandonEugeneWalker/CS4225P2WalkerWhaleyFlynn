package edu.westga.cs4225.project2.main;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;

/**
 * Determines the DegreeCentrality of each abstract.
 * 
 * @author Brandon Walker, Luke Whaley, Kevin flynn
 *
 */
public class DegreeCentralityFinder {
	private static ConcurrentHashMap<Text, ArrayListWritable<Text>> abstractWordMap = new ConcurrentHashMap<Text, ArrayListWritable<Text>>();
	private static volatile int REDUCE_COUNT = 0;
	private static volatile int MAP_COUNT = 0;

	/**
	 * Maps each abstract with each other.
	 * 
	 * @author Brandon Walker, Luke Whaley, Kevin Flynn
	 *
	 */
	public static class CentralityPrimaryMapper extends
			Mapper<Object, Text, Text, ArrayListWritable<Text>> {

		/**
		 * Maps each abstract with the list of its words.
		 * 
		 * @precondition none
		 * @postcondition the input data was mapped
		 */
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// if (MAP_COUNT % 1000 == 0) {
			// System.out.println("Map count: " + MAP_COUNT);
			// }

			String line = value.toString();
			String replacedCommad = line.replace(",", "");
			String replaceLeftBracket = replacedCommad.replace("[", "");
			String replaceRightBracket = replaceLeftBracket.replace("]", "");
			String replaceTab = replaceRightBracket.replace("\t", " ");
			String[] splitLine = replaceTab.split(" ");
			String currentAbstract = splitLine[0];

			Text abstractText = new Text(currentAbstract);
			ArrayListWritable<Text> words = new ArrayListWritable<Text>();
			// Text tokenText = new Text();

			for (int i = 1; i < splitLine.length; i++) {
				String currentToken = splitLine[i];
				// tokenText.set(currentToken);
				words.add(new Text(currentToken));
			}
			abstractWordMap.put(abstractText, words);
			context.write(abstractText, words);

			// MAP_COUNT++;

		}
	}

	/**
	 * Reduces the results of CentralityPrimaryMapper.
	 * 
	 * @author Brandon Walker, Luke Whaley, Kevin Flynn
	 *
	 */
	public static class CentralityPrimaryReducer
			extends
			Reducer<Text, ArrayListWritable<Text>, Text, ArrayListWritable<Text>> {

		/**
		 * Reduces the given input, outputting the abstract, and a list of each
		 * abstract that it shares words with.
		 * 
		 * @precondition none
		 * @postcondition the input is reduced
		 */
		@Override
		public void reduce(Text key, Iterable<ArrayListWritable<Text>> values,
				Context context) throws IOException, InterruptedException {
			// if (REDUCE_COUNT % 100 == 0) {
			// System.out.println("Reduce count: " + REDUCE_COUNT);
			// }

			ArrayListWritable<Text> commonAbstracts = new ArrayListWritable<Text>();
			ArrayListWritable<Text> currentTokens = abstractWordMap.get(key);
			Set<Text> abstractKeySet = abstractWordMap.keySet();

			for (Text currentKey : abstractKeySet) {
				ArrayListWritable<Text> currentCollection = abstractWordMap
						.get(currentKey);
				if (currentKey.equals(key)) {
					continue;
				} else if (this.collectionContainsAny(currentTokens,
						currentCollection)) {
					commonAbstracts.add(currentKey);
				}
			}
			context.write(key, commonAbstracts);

			// REDUCE_COUNT++;
		}

		private boolean collectionContainsAny(
				ArrayListWritable<Text> firstCollection,
				ArrayListWritable<Text> secondCollection) {

			boolean containsAny = false;
			for (Text currentKey : firstCollection) {
				if (secondCollection.contains(currentKey)) {
					containsAny = true;
					return true;
				}
			}
			return containsAny;
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
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (args.length != 2) {
			System.err.println("Usage: DegreeCentralityFinder <in> <out>");
			System.exit(2);
		}

		Job job1 = new Job(conf, "degree centrality finder first half");
		job1.setJarByClass(DegreeCentralityFinder.class);
		job1.setMapperClass(CentralityPrimaryMapper.class);
		job1.setCombinerClass(CentralityPrimaryReducer.class);
		job1.setReducerClass(CentralityPrimaryReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(ArrayListWritable.class);

		String outputFileName1 = args[1] + "/part1/"
				+ System.currentTimeMillis();
		String outputFileName2 = args[1] + "/part2/"
				+ System.currentTimeMillis();

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(outputFileName1));

		boolean results = false;

		try {
			results = job1.waitForCompletion(true);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}

		Job job2 = new Job(conf, "degree centrality finder second half");
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

		System.exit((results && results2) ? 0 : 1);

	}

}
