package base;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import clustering.DbscanFile;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.simplify.DouglasPeuckerLineSimplifier;

public class CuTs {

	static String inputFilePath="C:/Users/buraq/Google Drive/Brussels/PhD Work/working-folder/experiments/trucks_dataset/trucks273s.txt";
	static String inputFilePath1="D:/data/scaled";
	static String inputFilePath2="/home/faisal/Downloads/input/trucks273s.txt";
	static String outputFilePath="C:/Users/buraq/Google Drive/Brussels/PhD Work/working-folder/experiments/trucks_dataset/convoysOutput.txt";
	static String outputFilePath2="/home/faisal/Downloads/output/vcoda.txt";
	static int m=3;
	static double e=0.0006; // Range: 1/10^4 to 6/10^4
	static int k=180;
	static long clusteringCounter=0;
	static long convoyMiningCounter=0;
	static long totalCounter=0;
	static DbscanFile dbscan;
	static double delta = 10;
	
	public CuTs() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws FileNotFoundException {
		Iterator<CSVRecord> iter = getCSVIterator(inputFilePath);
		List<Coordinate[]> trajectories = getTrajectories(iter);
		List<Coordinate[]> simplifiedTrajectories = null;
		Iterator<Coordinate[]> iterTraj = trajectories.iterator();
		while(iterTraj.hasNext()){
			simplifiedTrajectories.add(DouglasPeuckerLineSimplifier.simplify(iterTraj.next(), delta));
		}
	}
	
	public static double computeLambda(List<Coordinate[]> simplifiedTrajectories, int k, double ratio){
		return 0;
	}
	
	public static List<Coordinate[]> getTrajectories(Iterator<CSVRecord> iter){
		return null;
	}
	public static Iterator<CSVRecord> getCSVIterator(String fileName) throws FileNotFoundException{
		File file = new File(fileName);
		int count=0;
		if(!file.exists()){
			throw new FileNotFoundException(fileName);
		}
		FileReader csvData = null;
		try {
			csvData = new FileReader(file);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		Iterator<CSVRecord> iterator = null;
		try {
			CSVParser records = CSVFormat.RFC4180.withHeader("oid","t","lat","long").parse(csvData);
			iterator = records.iterator();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return iterator;
	}
}
