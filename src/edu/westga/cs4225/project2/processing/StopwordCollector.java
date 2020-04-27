package edu.westga.cs4225.project2.processing;

import java.io.IOException;
import java.util.Collection;

/**
 * created to have interface of stopword collector
 * @author Brandon Walker, Luke Whaley, Kevin Flynn
 *
 */
public interface StopwordCollector {
	/**
	 * collect all stopword stirngs
	 * @return collection of strings
	 * @throws IOException if error occurs
	 */
	Collection<String> collect() throws IOException;
}
