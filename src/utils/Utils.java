package utils;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.math3.ml.clustering.Cluster;

import clustering.PointWrapper;
import base.Convoy;


public class Utils {

	public Utils() {
	}
	
	public static List<Convoy> clusterToConvoyList(List<Cluster<PointWrapper>> C){
		List<Convoy> V = new ArrayList<Convoy>();
		for(Cluster<PointWrapper> c : C){
			List<PointWrapper> pts = c.getPoints();
			Convoy v = Convoy.createConvoy(pts);
			V.add(v);
		}
		return V;
	}
	
	public static void writeConvoys(List<Convoy> Vpcc, String outputFilePath) throws IOException{
		Object [] FILE_HEADER = {"closedStatus","start","end"};
		String NEW_LINE_SEPARATOR = "\n";
		FileWriter fileWriter = null;
		CSVPrinter csvFilePrinter = null;
		CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator(NEW_LINE_SEPARATOR).withDelimiter(',');
		//initialize FileWriter object
		fileWriter = new FileWriter(outputFilePath);
		//initialize CSVPrinter object
		csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat);
		//Create CSV file header
		csvFilePrinter.printRecord(FILE_HEADER);
		int closedStatus=2;
		int row=0;
		for(Convoy v : Vpcc){
			System.out.print(v);
			if(v.isLeftOpen() && v.isRightOpen()){
				closedStatus=0;
			}else if(!v.isLeftOpen() && !v.isRightOpen()){
				closedStatus=2;
			}else if(v.isLeftOpen()){
				closedStatus=-1;
			}else if(v.isRightOpen()){
				closedStatus=1;
			}
			csvFilePrinter.printRecord(closedStatus,v.getStartTime(),v.getEndTime(),v.getObjs());
		}
		fileWriter.flush();
		fileWriter.close();
		csvFilePrinter.close();
		
		System.out.println("No. of convoys = "+Vpcc.size());
	}

}
