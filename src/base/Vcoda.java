package base;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;


import clustering.DbscanFileReader;
import org.apache.commons.math3.ml.clustering.Cluster;

import utils.Utils;
import clustering.PointWrapper;


public class Vcoda {
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
	static DbscanFileReader dbscan;
	
	public Vcoda() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws IOException {
		

//		HashMap<Integer,List<Cluster<PointWrapper>>> clusterMap = new DbscanFileReader().DBSCAN(inputFilePath2, m-1, e, 2);
//		
//		//*****************Apply PCCD algo on the list of clusters**************************************
//		List<Convoy> Vpcc = PCCD(clusterMap,k,m);
//		
//		//*******Print Vpcc************
//		Utils.writeConvoys(Vpcc, outputFilePath2);

		totalCounter = System.currentTimeMillis();
		inputFilePath = args[0];
		outputFilePath = args[1];
		m = Integer.parseInt(args[2]);
		k = Integer.parseInt(args[3]);
		e = Double.parseDouble(args[4]);
		convoyMiningCounter = System.currentTimeMillis();
		
		//****************Convoy Mining**************
		List<Convoy> Vpcc = PCCD(inputFilePath,k,m);
		//*******************************************
		
		convoyMiningCounter = System.currentTimeMillis() - convoyMiningCounter;
		
		//******************Print Vpcc***************
		Utils.writeConvoys(Vpcc, outputFilePath);
		//********************************************
		
		totalCounter = System.currentTimeMillis()-totalCounter;
		convoyMiningCounter = convoyMiningCounter - dbscan.getClusteringTime();
		
		PrintWriter pw = new PrintWriter(new File(outputFilePath.replace(".txt", "stats")+".txt"));
		pw.println("Total time taken in ms : "+totalCounter);
		System.out.println("Total time taken in ms : "+totalCounter);
		pw.println("Clustering time in ms : "+dbscan.getClusteringTime());
		System.out.println("Clustering time in ms : "+dbscan.getClusteringTime());
		pw.println("Convoy Mining time in ms : "+convoyMiningCounter);
		System.out.println("Convoy Mining time in ms : "+convoyMiningCounter);
		pw.flush();
		pw.close();

	}
	

	public static List<Convoy> PCCD(String inputFilePath, int k, int m) throws FileNotFoundException{
		List<Cluster<PointWrapper>> clusters=null;
		dbscan = new DbscanFileReader(inputFilePath);
//		int minTime=Collections.min(clusterMap.keySet());
//		int maxTime=Collections.max(clusterMap.keySet());
//		System.out.println("minTime="+minTime+"::maxTime="+maxTime);
		List<Convoy> Vnext = new ArrayList<Convoy>();
		List<Convoy> V = new ArrayList<Convoy>();
		List<Convoy> Vpcc = new ArrayList<Convoy>();
		List<Convoy> C = null;
		List<Cluster<PointWrapper>> CC = null;
		long t=1;
		while((CC=dbscan.getNextCluster(m-1,e,t))!=null){//maxTime;t++){
//			System.out.println("t="+t+" : No. of Clusters="+CC.size());
			Vnext=new ArrayList<Convoy>();
			if(CC!=null && CC.size()>0){
				C = Utils.clusterToConvoyList(CC);
			}
			else{
				t++;
				for(Convoy v:V){
					if(v.lifetime()>=k){
						Vpcc.add(v);
					}
				}
				V=new ArrayList<Convoy>();
				continue;
			}
			for(Convoy c : C){
				c.setMatched(false);c.setAbsorbed(false);
				System.out.println(c);
			}
			for(Convoy v : V){
				v.setExtended(false);v.setAbsorbed(false);
				for(Convoy c : C){
					if(c.size()>=m && v.intersection(c).size() >= m){ //extend (v,c)
						v.setExtended(true);c.setMatched(true);
						Convoy vext = new Convoy(v.intersection(c),v.getStartTime(),t);
						Vnext = updateVnext(Vnext, vext);
						if(v.isSubset(c)){v.setAbsorbed(true);}
						if(c.isSubset(v)){c.setAbsorbed(true);}
					}
				}
				if(!v.isAbsorbed()){
					if(v.lifetime() >= k){
						Vpcc.add(v);
					}
				}
			}
			for(Convoy c : C){
				if(!c.isAbsorbed()){
					Vnext=updateVnext(Vnext, c);
				}
			}
			V=Vnext;
			++t;
//			System.out.println("ts = "+t+", |V| = "+V.size()+", |Vpcc| = "+Vpcc.size());
		}
		for(Convoy v:V){
			if(v.lifetime()>=k){
				Vpcc.add(v);
			}
		}
		return Vpcc;
	}

	public static List<Convoy> updateVnext(List<Convoy> Vnext, Convoy vnew){
		boolean added=false;
		for(Convoy v : Vnext){
			if(v.hasSameObjs(vnew)){
				if(v.getStartTime()>vnew.getStartTime()){//v is a subconvoy of vnew
					Vnext.remove(v);
					Vnext.add(vnew);
					added=true;
				}
				else if(vnew.getStartTime()>v.getStartTime()){//vnew is a subconvoy of v *****different from vcoda
					added=true;
				}
			}
		}
		if(added==false){
			Vnext.add(vnew);
		}
		return Vnext;
	}
	
	public static List<Convoy> PCCD(HashMap<Integer, List<Cluster<PointWrapper>>> clusterMap, int k, int m){
		int minTime=Collections.min(clusterMap.keySet());
		int maxTime=Collections.max(clusterMap.keySet());
		System.out.println("minTime="+minTime+"::maxTime="+maxTime);
		List<Convoy> Vnext = new ArrayList<Convoy>();
		List<Convoy> V = new ArrayList<Convoy>();
		List<Convoy> Vpcc = new ArrayList<Convoy>();
		List<Convoy> C = null;
		for(int t=minTime;t<=2875;t++){//maxTime;t++){
			List<Cluster<PointWrapper>> CC = clusterMap.get(t);
			Vnext=new ArrayList<Convoy>();
			if(CC!=null && CC.size()>0){
				C = Utils.clusterToConvoyList(CC);
			}
			else{
				continue;
			}
			for(Convoy c : C){
				c.setMatched(false);c.setAbsorbed(false);c.setTime(t);
			}
			for(Convoy v : V){
				v.setExtended(false);v.setAbsorbed(false);
				for(Convoy c : C){
					if(c.size()>=m && v.intersection(c).size() >= m){ //extend (v,c)
						v.setExtended(true);c.setMatched(true);
						Convoy vext = new Convoy(v.intersection(c),v.getStartTime(),t);
						Vnext = updateVnext(Vnext, vext);
						if(v.isSubset(c)){v.setAbsorbed(true);}
						if(c.isSubset(v)){c.setAbsorbed(true);}
					}
				}
				if(!v.isAbsorbed()){
					if(v.lifetime() >= k){
						Vpcc.add(v);
					}
				}
			}
			for(Convoy c : C){
				if(!c.isAbsorbed()){
					Vnext=updateVnext(Vnext, c);
				}
			}
			V=Vnext;
//			System.out.println("ts = "+t+", |V| = "+V.size()+", |Vpcc| = "+Vpcc.size());
		}
		for(Convoy v:V){
			if(v.lifetime()>=k){
				Vpcc.add(v);
			}
		}
		return Vpcc;
	}

}
