package utils.DBSCAN;

import ca.pfv.spmf.algorithms.clustering.distanceFunctions.DistanceFunction;
import ca.pfv.spmf.patterns.cluster.DoubleArray;

public class MyEuclidianDistance extends DistanceFunction {
	/** the name of this distance function */
	static String NAME = "euclidian";
	
	/**
	 * Calculate the eucledian distance between two vectors of doubles.
	 * @param vector1 the first vector
	 * @param vector2 the second vector
	 * @return the distance
	 */
	public double calculateDistance(DoubleArray vector1, DoubleArray vector2) {
		double sum =0;	
		for(int i=0; i< vector1.data.length-2; i++){
			sum += Math.pow(vector1.data[i] - vector2.data[i], 2);
		}
		return Math.sqrt(sum);
	}

	@Override
	public String getName() {
		return NAME;
	}
	
}
