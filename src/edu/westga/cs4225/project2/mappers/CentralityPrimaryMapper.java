package edu.westga.cs4225.project2.mappers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;
import edu.westga.cs4225.project2.processing.SimilarityPreprocessor;

/**
 * Maps each abstract with each other.
 * 
 * @author Brandon Walker, Luke Whaley, Kevin Flynn
 *
 */
public class CentralityPrimaryMapper extends
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
	}

}
