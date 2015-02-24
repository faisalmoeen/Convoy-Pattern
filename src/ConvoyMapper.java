import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import parallel.VcodaNode;
import base.Convoy;
import utils.Utils;
import clustering.PointWrapper;


public class ConvoyMapper
extends Mapper<Object, Text, IntWritable, Text>{

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private int oid;
	private double lati;
	private double longi;

	int prevTime=-1;
	int currentTime=-1;
	int t=0;
	private int m;
	private double e;
	private int k;
	
	List<PointWrapper> clusterInput;
	DBSCANClusterer<PointWrapper> dbscan;
	HashMap<Integer,List<Cluster<PointWrapper>>> clusterMap;


	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		m = Integer.parseInt(conf.get("m"));
		k = Integer.parseInt(conf.get("k"));
		e = Double.parseDouble(conf.get("e"));
		clusterInput = new ArrayList<PointWrapper>();
		dbscan = new DBSCANClusterer<PointWrapper>(e, m-1);
		clusterMap = new HashMap<Integer,List<Cluster<PointWrapper>>>();

		super.setup(context);
	}
	
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] str = value.toString().split(",");
		oid = Integer.parseInt(str[0]);
		t = Integer.parseInt(str[1]);
		lati = Double.parseDouble(str[2]);
		longi = Double.parseDouble(str[3]);
		addPointForClustering(oid, t, lati, longi);
		
	}
	
	private void addPointForClustering(int oid, int t, double lati, double longi){
		if(currentTime!=-1 && t!=currentTime){
			prevTime=currentTime;
			if(clusterInput.size()>=m){
				List<Cluster<PointWrapper>> clusterResults = dbscan.cluster(clusterInput);
				if(clusterResults!=null && clusterResults.size()!=0){
					for(int k=0;k<clusterResults.size();k++){
						List<Integer> objs = Utils.clusterToConvoyList(clusterResults).get(k).getObjs();
						Collections.sort(objs);
					}
					clusterMap.put(currentTime, clusterResults);
				}
			}
			clusterInput.clear();
		}
		currentTime = t;
		PointWrapper p = new PointWrapper(oid,
				longi,
				lati);
		clusterInput.add(p);
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		//for last cluster
		if(clusterInput.size()>=m){
			List<Cluster<PointWrapper>> clusterResults = dbscan.cluster(clusterInput);
			System.out.println("clustering done");
			clusterMap.put(currentTime, clusterResults);
		}
		
		//*****************Apply PCCDNode algo on the list of clusters**************************************
		if(clusterMap.size()!=0){
			System.out.println(clusterMap.size());
			int minTime=Collections.min(clusterMap.keySet());
			int maxTime=Collections.max(clusterMap.keySet());
			List<Convoy> Vpcc = VcodaNode.PCCDNode(clusterMap,k,m,minTime,maxTime,1,2874);
			for(Convoy v:Vpcc){
				context.write(one, new Text(v.isLeftOpen()+","+v.isRightOpen()+
						","+v.getStartTime()+","+v.getEndTime()+
						","+v.getObjs().toString().replace("[", "").replace("]", "").replace(" ", "")));
			}
			System.out.println("Convoys out from partition ("+minTime+"-"+maxTime+")"+Vpcc.size());
		}
		
		super.cleanup(context);
	}
	
	
}