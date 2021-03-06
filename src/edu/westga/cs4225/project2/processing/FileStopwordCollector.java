package edu.westga.cs4225.project2.processing;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Scanner;

/**
 * Class created to store stop words in file 
 * 
 * @author Kevin Flynn, Luke Whaley, Brandon Walker
 *
 */
public class FileStopwordCollector implements StopwordCollector, Serializable {

	private static final long serialVersionUID = 6279914493079550937L;
	private Collection<String> tokens;
	
	/**
	 * Reads in InputStream trims the end of each 
	 * sets string to lower case then adds string to collection  
	 * @param stream stream of lines you wis hto read from
	 * @throws FileNotFoundException thrown if the file does not exist
	 */
	public FileStopwordCollector(InputStream stream) throws FileNotFoundException {
		this.tokens = new ArrayList<String>();
		try (Scanner scan = new Scanner(stream)) {
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
