package edu.westga.cs4225.project2.processing;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Scanner;

public class FileStopwordCollector implements StopwordCollector, Serializable {

	private static final long serialVersionUID = 6279914493079550937L;
	private Collection<String> tokens;
	
	public FileStopwordCollector(String filepath) throws FileNotFoundException {
		this.tokens = new ArrayList<String>();
		if (!filepath.startsWith("/")) {
			filepath = System.getProperty("user.dir") + "/" + filepath;
		}
		try (Scanner scan = new Scanner(new File(filepath))) {
			while (scan.hasNextLine()) {
				String word = scan.nextLine().trim().toLowerCase();
				this.tokens.add(word);
			}
		}
	}
	
	@Override
	public Collection<String> collect() throws IOException {
		return Collections.unmodifiableCollection(this.tokens);
	}

}
