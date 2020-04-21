package edu.westga.cs4225.project2.processing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

import edu.westga.cs4225.project2.datatypes.ArrayListWritable;
import edu.westga.cs4225.project2.stemmer.PorterStemmer;

public class AbstractProcessor {

	private Collection<String> stopwords;
	private PorterStemmer stemmer;
	private Set<Text> abstractStopwords;
	private Text abstractTitle;
	
	public AbstractProcessor(StopwordCollector collector) throws IOException {
		this.stopwords = new ArrayList<String>(collector.collect());
		this.stemmer = new PorterStemmer();
		this.abstractStopwords = new HashSet<Text>();
		this.abstractTitle = null;
	}
	
	public Text getKey() {
		return this.abstractTitle;
	}
	
	public ArrayListWritable<Text> getValue() {
		return new ArrayListWritable<Text>(new ArrayList<Text>(this.abstractStopwords));
	}
	
	public void process(String input) {
		String lowerCaseInput = input.toLowerCase().trim();
		String withoutNewlineInput = lowerCaseInput.replaceAll("\\\\n", "");
		this.filter(withoutNewlineInput);
	}
	
	private void filter(String input) {
		String[] parts = input.split(":");
		this.abstractTitle = new Text(parts[0].trim());
		String abstractContent = parts[1].trim();

		StringTokenizer tokenizer = new StringTokenizer(abstractContent);
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken().replaceAll("[^a-z]", "");
			if (!token.isEmpty() && !this.stopwords.contains(token)) {
				this.mapStemmedWord(this.abstractTitle.toString(), token);
			}
		}
	}
	
	private void mapStemmedWord(String title, String word) {
		char[] characters = word.toCharArray();
		for (Character character : characters) {
			this.stemmer.add(character);
		}
		this.stemmer.stem();
		String result = this.stemmer.toString();
		this.abstractStopwords.add(new Text(result));
	}
}
