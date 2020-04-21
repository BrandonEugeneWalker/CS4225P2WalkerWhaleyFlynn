package edu.westga.cs4225.project2.processing;

import java.io.IOException;
import java.util.Collection;

public interface StopwordCollector {

	Collection<String> collect() throws IOException;
}
