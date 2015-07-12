package cuts;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.ml.clustering.Cluster;

import utils.PointWrapperTemporalComparator;
import utils.Utils;
import utils.DBSCAN.DBSCANNlogN;
import utils.DBSCAN.MyDoubleArrayDBS;
import utils.DBSCAN.TrajDbscan;
import utils.DBSCAN.Trajectory;
import base.Convoy;
import ca.pfv.spmf.patterns.cluster.DoubleArray;
import clustering.DbscanFile;
import clustering.PointWrapper;

import com.goebl.simplify.Point;
import com.goebl.simplify.Simplify;



public class Cuts {
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
	static long trajSimplification=0;
	static long trajFiltering=0;
	static long trajDbscan=0;
	static long loadingTrajectories=0;
	static long startCheckPoint=0;
	static long checkPoint=0;
	static long checkPoint1=0;
	static long checkPoint2=0;	
	static long lambdaConvoyMining = 0;
	static long refinement=0;
	static DbscanFile dbscan;
	
	
	//	Simplify<Point> simplify = new Simplify<Point>(new MyPoint[0]);
	HashMap<Integer, Point[]> trajMap = new HashMap<Integer, Point[]>();
	public static void main(String[] args) throws FileNotFoundException {
		inputFilePath = args[0];
		outputFilePath = args[1];
		m = Integer.parseInt(args[2]);
		k = Integer.parseInt(args[3]);
		e = Double.parseDouble(args[4]);
		
		startCheckPoint = System.currentTimeMillis();
		double delta = computeDelta();
		HashMap<Integer, LinkedList<PointWrapper>> trajMap = getTrajectoriesFromFile(inputFilePath);
		checkPoint = System.currentTimeMillis();
		loadingTrajectories = checkPoint - startCheckPoint;
		HashMap<Integer, LinkedList<PointWrapper>> simplifiedTrajMap = DP(trajMap,delta,true);
		trajSimplification = System.currentTimeMillis()-checkPoint;
		checkPoint = System.currentTimeMillis();
		int lambda = computeLambda();
		List<Convoy> C = null;
		List<Convoy> V = new ArrayList<Convoy>();
		List<Convoy> Vnext = new ArrayList<Convoy>();
		List<Convoy> Vcand = new ArrayList<Convoy>();
		for(int i=1; i<=2874-lambda; i+=lambda){
			checkPoint1=System.currentTimeMillis();
			HashMap<Integer, LinkedList<PointWrapper>> G = filterTrajMap(simplifiedTrajMap,i,i+lambda-1);
			trajFiltering = trajFiltering+System.currentTimeMillis()-checkPoint1;
			checkPoint1 = System.currentTimeMillis();
			C = trajDBSCAN(G,e+2*delta,m,i,i+lambda-1);
			trajDbscan = trajDbscan+System.currentTimeMillis()-checkPoint1;
			for(Convoy c : C){
				c.setMatched(false);c.setAbsorbed(false);
//				System.out.println(c);
			}
			for(Convoy v : V){
				v.setExtended(false);v.setAbsorbed(false);
				for(Convoy c : C){
					if(c.size()>=m && v.intersection(c).size() >= m){ //extend (v,c)
						v.setExtended(true);c.setMatched(true);
						Convoy vext = new Convoy(v.intersection(c),v.getStartTime(),i+lambda-1);
						Vnext = updateVnext(Vnext, vext);
						if(v.isSubset(c)){v.setAbsorbed(true);}
						if(c.isSubset(v)){c.setAbsorbed(true);}
					}
				}
				if(!v.isAbsorbed()){
					if(v.lifetime() >= k){
						Vcand.add(v);
//						System.out.println(v);
					}
				}
			}
			for(Convoy c : C){
				if(!c.isAbsorbed()){
					Vnext=updateVnext(Vnext, c);
				}
			}
			V=Vnext;
			Vnext = new ArrayList<Convoy>();
		}
		lambdaConvoyMining = System.currentTimeMillis() - checkPoint;
		checkPoint = System.currentTimeMillis();
//		printConvoyList(Vcand);
		List<Convoy> result = CuTS_Refinement(Vcand, trajMap, m, k, e);
		printConvoyList(result);
		System.out.println("Candidate Convoys = "+Vcand.size());
		System.out.println("Result Convoys = "+result.size());
		refinement = System.currentTimeMillis() - checkPoint;
		totalCounter = System.currentTimeMillis() - startCheckPoint;
		System.out.println("Total Time = " + totalCounter+" ms");
		System.out.println("Traj Loading = "+loadingTrajectories+" ms");
		System.out.println("Traj Simplification = "+trajSimplification+" ms");
		System.out.println("Lambda Convoy Mining = "+lambdaConvoyMining+" ms");
		System.out.println("Traj Filtering = "+trajFiltering+" ms");
		System.out.println("Traj DBSCAN = "+trajDbscan+" ms");
		System.out.println("Lambda only Convoy Mining = "+(lambdaConvoyMining-trajFiltering-trajDbscan)+" ms");
		System.out.println("Refinement = "+refinement+" ms");
		
//		printConvoyList(result);
//		System.out.println("Lambda convoys no. = "+Vcand.size());
//		System.out.println("No. of Convoys = "+result.size());
		return;
		
	}
	
	public static void printConvoyList(List<Convoy> V){
		System.out.println("Printing lambda convoys now: ");
		for(Convoy v:V){
			System.out.println(v);
		}
		System.out.println("No. of Convoys = "+V.size());
	}
	public static List<Convoy> CuTS_Refinement(List<Convoy> Vcand, HashMap<Integer, LinkedList<PointWrapper>> trajMap, int m, int k, double e){
		HashMap<Long,List<PointWrapper>> pointsMap;
		List<Convoy> result = new ArrayList<Convoy>();
		List<Convoy> partialResult = new ArrayList<Convoy>();
		for(Convoy v:Vcand){
			pointsMap = getPointsForCMC(v.getObjs(),v.getStartTime(),v.getEndTime(), trajMap);
			try {
				partialResult = CMC(pointsMap,e,m,k);
				if(partialResult!=null && partialResult.size()>0){
					result.addAll(partialResult);
				}
			} catch (NumberFormatException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		return result;
	}
	
	public static List<Convoy> CMC(HashMap<Long,List<PointWrapper>> pointsMap, double e, int m, int k) throws NumberFormatException, IOException{
		List<Convoy> Vnext = new ArrayList<Convoy>();
		List<Convoy> V = new ArrayList<Convoy>();
		List<Convoy> Vpcc = new ArrayList<Convoy>();
		List<Convoy> C = new ArrayList<Convoy>();
		List<Cluster<PointWrapper>> CC = null;
		long t=1;
		long checkPoint=0;
		
		List<Long> times = new ArrayList<Long>(pointsMap.keySet());
		Collections.sort(times);
		Iterator<Long> i = times.iterator();
		while(i.hasNext()){//maxTime;t++){
			t=i.next();
//			System.out.println("Current time:"+t);
			DBSCANNlogN algo = new DBSCANNlogN();  
			checkPoint=System.currentTimeMillis();
			///convert list to arrays for nlogn dbscan
			List<DoubleArray> points= new ArrayList<DoubleArray>();
			Iterator<PointWrapper> pm = pointsMap.get(t).iterator();
			while(pm.hasNext()){
				PointWrapper pw = pm.next();
				points.add(pw.getDoubleArray()); 
			}
			List<ca.pfv.spmf.patterns.cluster.Cluster> clusters = algo.runAlgorithmOnArray(m, e, points);
			clusteringCounter+=(System.currentTimeMillis()-checkPoint);
			checkPoint=System.currentTimeMillis();
			//TODO: fill C with clusters
//			System.out.println("t="+t+" : No. of Clusters="+clusters.size());
			Vnext=new ArrayList<Convoy>();
			if(clusters!=null && clusters.size()>0){
				C = Utils.clustersToConvoyList(clusters);
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
//				System.out.println(c);
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
//			System.out.println("No. of clusters = "+clusters.size());
			//algo.printStatistics();
			convoyMiningCounter+=(System.currentTimeMillis()-checkPoint);
		}
		for(Convoy v:V){
			if(v.lifetime()>=k){
				Vpcc.add(v);
			}
		}
		return Vpcc;
	}
	public static HashMap<Long,List<PointWrapper>> getPointsForCMC(List<Integer> objs, long start, long end, HashMap<Integer, LinkedList<PointWrapper>> trajMap){
		List<PointWrapper> list = new ArrayList<PointWrapper>();
		HashMap<Long,List<PointWrapper>> map = new HashMap<Long,List<PointWrapper>>();
		Iterator<Integer> i = objs.iterator();
		while(i.hasNext()){
			int oid = i.next();
			LinkedList<PointWrapper> traj = trajMap.get(oid);
			Iterator<PointWrapper> j = traj.iterator();
			while(j.hasNext()){
				PointWrapper p = j.next();
				if(p.getTime()>=start && p.getTime()<=end){
					if(map.containsKey(p.getTime())){
						map.get(p.getTime()).add(p);
					}
					else{
						list = new ArrayList<PointWrapper>();
						list.add(p);
						map.put(p.getTime(), list);
					}
				}
			}
		}
		return map;
	}
	public static List<Convoy> trajDBSCAN(HashMap<Integer, LinkedList<PointWrapper>> trajMap, double e, int m, int start, int end){
		Iterator<Integer> keys = trajMap.keySet().iterator();
		List<Trajectory> trajectories = new ArrayList<Trajectory>();
		while(keys.hasNext()){
			int key=keys.next();
			trajectories.add(new Trajectory(key, trajMap.get(key)));
		}
		TrajDbscan<Trajectory> trajDbscan = new TrajDbscan<>(e, m);
		List<Cluster<Trajectory>> clusters = trajDbscan.cluster(trajectories);
		return Utils.trajClusterToConvoyList(clusters);
	}
	public static HashMap<Integer, LinkedList<PointWrapper>> filterTrajMap(HashMap<Integer, LinkedList<PointWrapper>> trajMap,int start,int end){
		Iterator<Integer> keys = trajMap.keySet().iterator();
		HashMap<Integer, LinkedList<PointWrapper>> partTrajMap = new HashMap<Integer, LinkedList<PointWrapper>>();
		while(keys.hasNext()){
			int key = keys.next();
			Iterator<PointWrapper> points = trajMap.get(key).iterator();
			LinkedList<PointWrapper> partTraj = new LinkedList<PointWrapper>();
			PointWrapper p1=null;
			PointWrapper p2=null;
			while(points.hasNext()){
				PointWrapper p = points.next();
				if(p1==null){
					p1=p;
					if(p1.getTime()<=end && p1.getTime()>=start){
						partTraj.add(p1);
					}
				}else{
					p2=p;
				}
				if(p1!=null && p2!=null){
					if(p2.getTime()<=end && p2.getTime()>=start){
						if(p1.getTime()<start){
							partTraj.add(p1);
						}
						partTraj.add(p2);
					}
					else if(p2.getTime()>end){
						if(p1.getTime()<start){
							partTraj.add(p1);
							partTraj.add(p2);
						}
						else if(p1.getTime()<=end){
							partTraj.add(p2);
						}
						break;
					}
					p1=p2;
				}
			}
			if(partTraj!=null && partTraj.size()>0){
				partTrajMap.put(key, partTraj);
			}
			
		}
		return partTrajMap;
	}
	public static HashMap<Integer, LinkedList<PointWrapper>> DP(HashMap<Integer, LinkedList<PointWrapper>> map, double delta, boolean highQuality){
		Simplify<Point> simplify = new Simplify<Point>(new PointWrapper[0]);
		Iterator<Integer> keys = map.keySet().iterator();
		HashMap<Integer, LinkedList<PointWrapper>> simplifiedTrajMap = new HashMap<Integer, LinkedList<PointWrapper>>();
		int key=0;
		PointWrapperTemporalComparator temporalComp = new PointWrapperTemporalComparator();
		while(keys.hasNext()){
			key = keys.next();
			LinkedList<PointWrapper> traj = map.get(key);
			LinkedList<PointWrapper> simptraj = new LinkedList<PointWrapper>();
			PointWrapper[] lessPoints = (PointWrapper[]) simplify.simplify(traj.toArray(new PointWrapper[traj.size()]), delta, highQuality);
			Arrays.sort(lessPoints,temporalComp);
			for (PointWrapper p:lessPoints){
				simptraj.add(p);
			}
			simplifiedTrajMap.put(key, simptraj);
		}
		return simplifiedTrajMap;
	}
	
	public static int computeLambda(){
		return 1;
	}
	public static double computeDelta(){
		return 0.0000;// 0.0002;
	}
	public static void readTrajectories(){
		
	}
	
	public static HashMap<Integer, LinkedList<PointWrapper>> getTrajectoriesFromFile(String inputFilePath) throws FileNotFoundException{
		File file = new File(inputFilePath);
		int count=0;
		if(!file.exists()){
			throw new FileNotFoundException(inputFilePath);
		}
		HashMap<Integer, LinkedList<PointWrapper>> map = new HashMap<Integer, LinkedList<PointWrapper>>();
		Reader csvData = null;
		Iterable<CSVRecord> records = null;
		CSVRecord record=null;
		Iterator<CSVRecord> iterator = null;
		try {
			csvData = new FileReader(file);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		try {
			records = CSVFormat.RFC4180.withHeader("oid","t","lat","long").parse(csvData);
			iterator = records.iterator();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		PointWrapper point=null;
		while(iterator.hasNext()) {
			point = new PointWrapper();
			record = iterator.next();
			point.setX(Double.parseDouble(record.get("long")));
			point.setY(Double.parseDouble(record.get("lat")));
			point.setOid(Integer.parseInt(record.get("oid")));
			point.setTime(Integer.parseInt(record.get("t")));
			if(map.containsKey(point.getOid())){
				map.get(point.getOid()).add(point);
			}
			else{
				LinkedList<PointWrapper> l = new LinkedList<PointWrapper>();
				l.add(point);
				map.put(point.getOid(), l);
			}
		}
		return map;
	}
	
	public static List<Convoy> updateVnext(List<Convoy> Vnext, Convoy vnew){
		boolean added=false;
		List<Convoy> toRemove = new ArrayList<>();
		for(Convoy v : Vnext){
			if(v.hasSameObjs(vnew)){
				if(v.getStartTime()>vnew.getStartTime()){//v is a subconvoy of vnew
					toRemove.add(v);
//					Vnext.add(vnew);
//					added=true;
				}
				else if(vnew.getStartTime()>v.getStartTime()){//vnew is a subconvoy of v *****different from vcoda
					added=true;
				}
			}
		}
		if(added==false){
			Vnext.add(vnew);
		}
		for(Convoy v:toRemove){
			Vnext.remove(v);
		}
		return Vnext;
	}
}
