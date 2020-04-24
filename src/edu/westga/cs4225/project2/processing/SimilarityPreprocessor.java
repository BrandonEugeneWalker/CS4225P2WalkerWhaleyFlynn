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

public class SimilarityPreprocessor {

	private HashMap<String, ArrayList<String>> data;
	
	public SimilarityPreprocessor() {
		this.data = null;
	}
	
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
	
	public String[] getContents(String line) {
		String formatted = line.replaceAll("\\[", ",").replaceAll("\\]", "").replaceAll("\\s+", "");
		String[] contents = formatted.split(",");
		return contents;
	}
	
	public HashMap<String, ArrayList<String>> getData() {
		return this.data;
	}

}
