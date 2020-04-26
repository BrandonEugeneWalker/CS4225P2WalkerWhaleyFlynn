package edu.westga.cs4225.project2.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Maps on the results of the first map/reduce job for finding the centrality of
 * each abstract.
 * 
 * @author Brandon Walker, Luke Whaley, Kevin Flynn
 *
 */
public class CentralitySecondaryMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable ONE = new IntWritable(1);

	/**
	 * Using the results from the first job, it maps each number of another abstract
	 * attached to the key abstract.
	 * 
	 * @precondition none
	 * @postcondition the input is mapped
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String replacedCommad = line.replace(",", "");
		String replaceLeftBracket = replacedCommad.replace("[", "");
		String replaceRightBracket = replaceLeftBracket.replace("]", "");
		String replaceTab = replaceRightBracket.replace("\t", " ");
		String[] splitLine = replaceTab.split(" ");
		String keyAbstract = splitLine[0];
		Text keyAbstractText = new Text(keyAbstract);

		for (int i = 1; i < splitLine.length; i++) {
			context.write(keyAbstractText, this.ONE);
		}
	}
}
