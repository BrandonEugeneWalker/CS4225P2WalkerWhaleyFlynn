package edu.westga.cs4225.project2.input;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Custom record reader that reads the data
 * from the input files.
 * 
 * @author Luke Whaley, Brandon Walker, Kevin Flynn
 *
 */
public class MyNLineRecordReader extends RecordReader<LongWritable, Text> {

	private LongWritable key;
	private Text value;
	
	private Configuration config;
	
	private FileSplit fsplit;
	private FSDataInputStream fsinstream;
	private FileSystem fs;
	
	private long splitstart = 0;
	private long splitlen = 0;
	private long bytesread = 0;
	
	private BufferedReader buffer;
	private int nlinestoread = 2;
	private boolean isComplete = false;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fsplit = (FileSplit) split;
		this.config = context.getConfiguration();
		this.fs = this.fsplit.getPath().getFileSystem(this.config);
		this.fsinstream = this.fs.open(this.fsplit.getPath());
		this.buffer = new BufferedReader(new InputStreamReader(this.fsinstream));

		this.splitstart = this.fsplit.getStart();
		this.splitlen = this.fsplit.getLength();
		
		if (this.splitstart == 0) {
			this.splitlen++;
		} else {
			this.splitstart++;
		}
		
		this.fsinstream.skip(this.splitstart);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (this.bytesread >= this.splitlen) {
			this.isComplete = true;
			return false;
		} else {
			String line;
			if ((line = this.readNLines()) != null) {
				this.value = new Text(line);
				this.key = new LongWritable(this.splitstart);
				return true;
			} else {
				this.isComplete = true;
				return false;
			}
		}
	}
	
	/**
	 * Reads two lines and interprets it as one line.
	 * 
	 * @return the two lines as one line.
	 * @throws IOException if an error occurs during the read operation.
	 */
	private String readNLines() throws IOException {
		String nlines = null;
		String line = null;
		for (int i = 0; (i < this.nlinestoread) && ((line = this.buffer.readLine()) != null); i++) {
			if (nlines == null) {
				nlines = line;
				this.bytesread += line.getBytes().length;
				this.buffer.mark(8192);
			} else {
				nlines = nlines.concat(line);
				this.buffer.reset();
			}
		}
		return nlines;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return this.key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return this.value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return this.isComplete ? 1.0f : 0.0f;
	}
	
	@Override
	public void close() throws IOException {
	}
}
