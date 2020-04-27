package edu.westga.cs4225.project2.main;

import java.io.IOException;

import edu.westga.cs4225.project2.main.similarity.PartSixSimilarity;

/**
 * The entry point into the application
 * @author Luke Whaley, Brandon Walker, Kevin Flynn 
 *
 */
public class Main {

	/**
	 * Runs all the jobs.
	 * @param args the arguments
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		if (args.length != 3) {
			System.err.println("Usage: Infometrics <in> <out> <stopword-file>");
			System.exit(2);
		}

		String partTwoInputLocation = args[0];
		String initialOutputLocation = args[1] + System.currentTimeMillis();
		String stopwordInputLocation = args[2];

		String partTwoOutputLocation = initialOutputLocation + "/number2";
		String partThreeOutputLocation = initialOutputLocation + "/number3";
		String partFourOutputLocation = initialOutputLocation + "/number4";
		String partFiveOutputLocation = initialOutputLocation + "/number5";
		String partSixOutputLocation = initialOutputLocation + "/number6";

		// String partThreeInputLocation;
		// String partFourInputLocation;
		String partFiveInputLocation = partFourOutputLocation + "/part2";
		// String partSixInputLocation;

		// String wordCountLocation = partThreeOutputLocation + "/part-r-00000";
		boolean partTwoResults = false;
		boolean partThreeResults = false;
		boolean partFourResults = false;
		boolean partFiveResults = false;
		boolean partSixResults = false;

		partTwoResults = PartTwoInfometrics.runPartTwo(partTwoInputLocation, partTwoOutputLocation,
				stopwordInputLocation);

		if (partTwoResults) {
			partThreeResults = PartThreeAbstractCount.runPartThree(partTwoOutputLocation, partThreeOutputLocation);
		}

		if (partThreeResults) {
			partFourResults = PartFourDegreeCentralityFinder.runPartFour(partTwoOutputLocation, partFourOutputLocation);
		}

		if (partThreeResults && partFourResults) {
			partFiveResults = PartFiveStandardizedScore.runPartFive(partThreeOutputLocation, partFiveInputLocation,
					partFiveOutputLocation);
		}

		if (partTwoResults) {
			partSixResults = PartSixSimilarity.runPartSix(partTwoOutputLocation, partSixOutputLocation);
		}

		if (partTwoResults && partThreeResults && partFourResults && partFiveResults && partSixResults) {
			System.exit(0);
		} else {
			System.err.println("One or more jobs failed, check output.");
			System.exit(2);
		}

	}
}
