package utils.DBSCAN;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import ca.pfv.spmf.algorithms.clustering.distanceFunctions.DistanceEuclidian;
import ca.pfv.spmf.algorithms.clustering.distanceFunctions.DistanceFunction;
import ca.pfv.spmf.datastructures.kdtree.KDTree;
import ca.pfv.spmf.patterns.cluster.Cluster;
import ca.pfv.spmf.patterns.cluster.DoubleArray;
import ca.pfv.spmf.tools.MemoryLogger;
import ca.pfv.spmf.algorithms.clustering.dbscan.AlgoDBSCAN;
import ca.pfv.spmf.algorithms.clustering.dbscan.DoubleArrayDBS;

public class DBSCANNlogN extends AlgoDBSCAN{

	// The list of clusters generated
		protected List<Cluster> clusters = null;
		BufferedReader reader;

		// For statistics
		protected long startTimestamp; // the start time of the latest execution
		protected long endTimestamp;  // the end time of the latest execution
		long numberOfNoisePoints; // the number of iterations that was performed
		
		/* The distance function to be used for clustering */
		DistanceFunction distanceFunction = new DistanceEuclidian(); 
		
		/* This KD-Tree is used to index the data points for fast access to points in the epsilon radius*/
		MyKDTree kdtree;

		/**
		 * Default constructor
		 */
		public DBSCANNlogN() { 
			
		}
		
		public DBSCANNlogN(String fileName) throws FileNotFoundException { 
			reader = new BufferedReader(new FileReader(fileName));
		}
		

		
		/**
		 * Run the DBSCAN algorithm
		 * @param inputFile an input file path containing a list of vectors of double values
		 * @param minPts  the minimum number of points (see DBScan article)
		 * @param epsilon  the epsilon distance (see DBScan article)
		 * @param seaparator  the string that is used to separate double values on each line of the input file (default: single space)
		 * @return a list of clusters (some of them may be empty)
		 * @throws IOException exception if an error while writing the file occurs
		 */
		public List<Cluster> runAlgorithmOnArray(int minPts, double epsilon, List<DoubleArray> points) throws NumberFormatException, IOException {
			
			// record the start time
			startTimestamp =  System.currentTimeMillis();
			// reset the number of noise points to 0
			numberOfNoisePoints =0;
			
			// Structure to store the vectors from the file
//			List<DoubleArray> points = new ArrayList<DoubleArray>();
			
			// build kd-tree
			kdtree = new MyKDTree();
			kdtree.buildtree(points);
			
			// For debugging, you can print the KD-Tree by uncommenting the following line:
//			System.out.println(kdtree.toString());
			
			// Create a single cluster and return it 
			clusters = new ArrayList<Cluster>();
			
			
			// For each point in the dataset
			for(DoubleArray point: points) {
				// if the node is already visited, we skip it
				MyDoubleArrayDBS pointDBS = (MyDoubleArrayDBS) point;
				if(pointDBS.visited == true) {
					continue;
				}
				
				// mark the point as visited
				pointDBS.visited = true;
				
				// find the neighboors of this point
				List<DoubleArray> neighboors = kdtree.pointsWithinRadiusOf(pointDBS, epsilon);
				// if it is not noise
				if(neighboors.size() >= minPts -1) { // - 1 because we don't count the point itself in its neighborood
					
					// create a new cluster
					Cluster cluster = new Cluster();
					clusters.add(cluster);
					
					// transitively add all points that can be reached
					expandCluster(pointDBS, neighboors, cluster, epsilon, minPts);
				}else {
					// it is noise
					numberOfNoisePoints++;
				}
				
			}
			
			kdtree = null;
			
			// check memory usage
			MemoryLogger.getInstance().checkMemory();
			
			// record end time
			endTimestamp =  System.currentTimeMillis();
			
			// return the clusters
			return clusters;
		}
		
		
		/**
		 * The DBScan expandCluster() method
		 * @param currentPoint the current point
		 * @param neighboors the neighboors of the current point
		 * @param cluster the current cluster
		 * @param epsilon the epsilon parameter
		 * @param minPts the minPts parameter
		 */
		private void expandCluster(MyDoubleArrayDBS currentPoint,
				List<DoubleArray> neighboors, Cluster cluster, double epsilon, int minPts) {	
			// add the current point to the cluster
			cluster.addVector(currentPoint);
			
			// for each neighboor
			for(DoubleArray newPoint: neighboors) {
				MyDoubleArrayDBS newPointDBS = (MyDoubleArrayDBS) newPoint;
				
				// if this point has not been visited yet
				if(newPointDBS.visited == false) {
						
					// mark the point as visited
					newPointDBS.visited = true;
					
					// find the neighboors of this point
					List<DoubleArray> newNeighboors = kdtree.pointsWithinRadiusOf(newPointDBS, epsilon);
					
					// if this point is not noise
					if(newNeighboors.size() >= minPts - 1) { // - 1 because we don't count the point itself in its neighborood
						expandCluster(newPointDBS, newNeighboors, cluster, epsilon, minPts);
					}else {
						cluster.addVector(newPointDBS);
						// it is noise
						numberOfNoisePoints++;
					}
				}
			}

			// check memory usage
			MemoryLogger.getInstance().checkMemory();
		}

		/**
		 * Save the clusters to an output file
		 * @param output the output file path
		 * @throws IOException exception if there is some writing error.
		 */
		public void saveToFile(String output) throws IOException {
			BufferedWriter writer = new BufferedWriter(new FileWriter(output));
			// for each cluster
			for(int i=0; i< clusters.size(); i++){
				// if the cluster is not empty
				if(clusters.get(i).getVectors().size() >= 1){
					// write the cluster
					writer.write(clusters.get(i).toString());
					// if not the last cluster, add a line return
					if(i < clusters.size()-1){
						writer.newLine();
					}
				}
			}
			// close the file
			writer.close();
		}
		
		/**
		 * Print statistics of the latest execution to System.out.
		 */
		public void printStatistics() {
			System.out.println("========== DBSCAN - STATS ============");
			System.out.println(" Total time ~: " + (endTimestamp - startTimestamp)
					+ " ms");
			System.out.println(" Max memory:" + MemoryLogger.getInstance().getMaxMemory() + " mb ");
			System.out.println(" Number of noise points: " + numberOfNoisePoints);
			System.out.println("=====================================");
		}

}
