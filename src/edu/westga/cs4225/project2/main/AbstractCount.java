package edu.westga.cs4225.project2.main;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Scanner;

import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.westga.cs4225.project2.main.StandardizeScore.MyStandardizeMapper;
import edu.westga.cs4225.project2.main.StandardizeScore.MyStandardizeReducer;
import edu.westga.cs4225.project2.processing.FileStopwordCollector;

/**
 * 
 * @author Kevin Flynn
 *
 */
public class AbstractCount {

	/**
	 * This is the mapper class that maps all of the data.
	 * The map method counts all of the kmers in the line.
	 * 
	 * @author Kevin Flynn
	 *
	 */
	public static class MyCountMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		
		private static IntWritable ONE = new IntWritable(1);

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Text Collection_Size = new Text("Collection_Size");
			String serializedCollector = context.getConfiguration().get("COLLECTOR");
			ObjectInputStream objectinput = new ObjectInputStream(new ByteArrayInputStream(Base64.decodeBase64(serializedCollector)));
			context.write(Collection_Size, ONE);
			
		}
	}

	/**
	 * This is the reducer class.
	 * It counts all of occurrences of the given kmer.
	 * 
	 * @author Kevin Flynn
	 *
	 */
	public static class MyCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();
		@Override
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
	

	
	private static void readCollectionSize(Path collectionSizePath){
		File file = new File(collectionSizePath.toString()+ "/part-r-00000");
		try {
		      Scanner myReader = new Scanner(file);
		      while (myReader.hasNextLine()) {
		        String data = myReader.nextLine();
		        String[] allInput = data.split("\t");
		        if (allInput.length == 2){
			        System.out.println(allInput);
			        int value = Integer.parseInt(allInput[1]);
			        AbstractSize.COLLECTION_SIZE = value;
		        }

		      }
		      myReader.close();
		    } catch (FileNotFoundException e) {
		      System.out.println("An error occurred.");
		      e.printStackTrace();
		}
	}
	
	
	
	/**
	 * Starting point for the application. Takes 3 arguments. 
	 * Arguments = Input Directory, Output Directory, K-Count.
	 * 
	 * @param args the program arguments.
	 * @throws Exception if any errors occur.
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: Infometrics <in> <out> <stopword-file>");
			//ToolRunner.printGenericCommandUsage(System.err);
			//System.exit(2);
		}
		
		FileStopwordCollector collector = new FileStopwordCollector(args[0]);
		ByteArrayOutputStream byteoutput = new ByteArrayOutputStream();
		ObjectOutputStream objectoutput = new ObjectOutputStream(byteoutput);
		
		objectoutput.writeObject(collector);
		objectoutput.close();
		String serializedCollector = new String(Base64.encodeBase64(byteoutput.toByteArray()));
		
		Configuration conf = new Configuration();
		conf.set("COLLECTOR", serializedCollector);

		Job job = Job.getInstance(conf, "Abstract Count");
		
		job.setJarByClass(AbstractCount.class);
		job.setMapperClass(MyCountMapper.class);
		job.setCombinerClass(MyCountReducer.class);
		job.setReducerClass(MyCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//arxiv.txr
		FileInputFormat.addInputPath(job, new Path(args[1]));
		String outputPath = args[3] + "/part3/" +  "out" + System.currentTimeMillis();
		Path collectionSizePath = new Path(outputPath);
		FileOutputFormat.setOutputPath(job, collectionSizePath);	
		boolean result = false;
		
		try {
			result = job.waitForCompletion(true);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}		
			boolean result2 = false;
			readCollectionSize(collectionSizePath);

			Configuration conf2 = new Configuration();		
			Job job2 = Job.getInstance(conf2, "Standardize count");
			
			NLineInputFormat.setNumLinesPerSplit(job2, 0);
			job2.setJarByClass(AbstractCount.class);
			job2.setMapperClass(MyStandardizeMapper.class);
			job2.setCombinerClass(MyStandardizeReducer.class);
			job2.setReducerClass(MyStandardizeReducer.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);
			//results
			FileInputFormat.addInputPath(job2, new Path(args[2]));

			FileOutputFormat.setOutputPath(job2, new Path(args[3] +"/part5/" +"out" + System.currentTimeMillis()));
			try {
				result2 = job2.waitForCompletion(true);
			} catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
			
		System.exit( (result && result2) ? 0 : 1);		

	}

}
