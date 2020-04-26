package edu.westga.cs4225.project2.mappers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.commons.net.util.Base64;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;
import edu.westga.cs4225.project2.processing.AbstractProcessor;
import edu.westga.cs4225.project2.processing.FileStopwordCollector;

/**
 * This is the mapper class that maps all of the data.
 * 
 * @author Luke Whaley
 *
 */
public class DataCleaningMapper extends Mapper<Object, Text, Text, ArrayListWritable<Text>> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String serializedCollector = context.getConfiguration().get("COLLECTOR");
		ObjectInputStream objectinput = new ObjectInputStream(
				new ByteArrayInputStream(Base64.decodeBase64(serializedCollector)));
		try {
			FileStopwordCollector collector = (FileStopwordCollector) objectinput.readObject();
			AbstractProcessor processor = new AbstractProcessor(collector);
			processor.process(value.toString());
			context.write(processor.getKey(), processor.getValue());
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException();
		} finally {
			objectinput.close();
		}
	}
}
