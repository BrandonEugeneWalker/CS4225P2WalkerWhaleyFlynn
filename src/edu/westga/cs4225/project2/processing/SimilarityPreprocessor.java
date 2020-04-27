package edu.westga.cs4225.project2.processing;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

/**
 * Class created to process similarity in words
 * 
 * @author Luke Whaley, Brandon Walker, Kevin Flynn
 *
 */
public class SimilarityPreprocessor {

	private HashMap<String, ArrayList<String>> data;
	
	/**
	 * Class created to store data similarities values
	 */
	public SimilarityPreprocessor() {
		this.data = null;
	}
	/**
	 * reads in file and trims each line then processes each line
	 * @param filepath path of the desire file
	 * @throws FileNotFoundException thrown when file not found 
	 * @throws InterruptedException thrown when process interupted
	 */
	public SimilarityPreprocessor(String filepath) throws FileNotFoundException, InterruptedException {
		this.data = new HashMap<String, ArrayList<String>>();
		if (!filepath.startsWith("/")) {
			filepath = System.getProperty("user.dir") + "/" + filepath;
		}
		try (Scanner scan = new Scanner(new File(filepath))) {
			while (scan.hasNextLine()) {
				String line = scan.nextLine().trim();
				this.process(line);
			}
		}
	}

	private void process(String line) {
		String[] contents = this.getContents(line);
		String abstractTitle = contents[0];
		ArrayList<String> words = new ArrayList<String>();
		for (int i = 1; i < contents.length; i++) {
			words.add(contents[i]);
		}
		this.data.put(abstractTitle, words);
	}
	
	/**
	 * replaces all spaces and brackets with either , or spaces
	 * @param line the line you wish to alter
	 * @return string array of revised lines
	 */
	public String[] getContents(String line) {
		String formatted = line.replaceAll("\\[", ",").replaceAll("\\]", "").replaceAll("\\s+", "");
		String[] contents = formatted.split(",");
		return contents;
	}
	
	/**
	 * gets the revised data
	 * @return hash map of revised data
	 */
	public HashMap<String, ArrayList<String>> getData() {
		return this.data;
	}

}
