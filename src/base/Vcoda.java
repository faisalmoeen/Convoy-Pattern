package base;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;



import org.apache.commons.math3.ml.clustering.Cluster;

import utils.Utils;
import clustering.DbscanFile;
import clustering.PointWrapper;


public class Vcoda {
	static String inputFilePath="C:/Users/buraq/Google Drive/Brussels/PhD Work/working-folder/experiments/trucks_dataset/trucks273s.txt";
	static String inputFilePath1="D:/data/scaled";
	static String outputFilePath="C:/Users/buraq/Google Drive/Brussels/PhD Work/working-folder/experiments/trucks_dataset/convoysOutput.txt";
	static int m=3;
	static double e=0.0006; // Range: 1/10^4 to 6/10^4
	static int k=180;
	
	public Vcoda() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws IOException {
		
		HashMap<Integer,List<Cluster<PointWrapper>>> clusterMap = new DbscanFile().DBSCAN(inputFilePath, m-1, e, 2);
		
		//*****************Apply PCCD algo on the list of clusters**************************************
		List<Convoy> Vpcc = PCCD(clusterMap,k,m);
		
		//*******Print Vpcc************
		Utils.writeConvoys(Vpcc, outputFilePath);

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
