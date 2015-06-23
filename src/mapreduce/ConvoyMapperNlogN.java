package mapreduce;
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
import utils.DBSCAN.DBSCANNlogN;
import utils.DBSCAN.MyDoubleArrayDBS;
import ca.pfv.spmf.patterns.cluster.DoubleArray;
import clustering.PointWrapper;


public class ConvoyMapperNlogN
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
	
	List<DoubleArray> clusterInput;
	DBSCANClusterer<PointWrapper> dbscan;
	VcodaNode vcodaNode;
	int globalMinTime,globalMaxTime;
	List<Convoy> Vpcc = null;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		long start = context.getCounter(ConvoyCounters.MAP_START).getValue();
		if(start==0){
			context.getCounter(ConvoyCounters.MAP_START).setValue(System.currentTimeMillis());
		}
		Configuration conf = context.getConfiguration();
		m = Integer.parseInt(conf.get("m"));
		k = Integer.parseInt(conf.get("k"));
		e = Double.parseDouble(conf.get("e"));
		globalMinTime = Integer.parseInt(conf.get("gMinTime"));
		globalMaxTime = Integer.parseInt(conf.get("gMaxTime"));
		clusterInput = new ArrayList<DoubleArray>();
		dbscan = new DBSCANClusterer<PointWrapper>(e, m-1);
		vcodaNode = new VcodaNode(globalMinTime, globalMaxTime);
		super.setup(context);
	}
	
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] str = value.toString().split(",");
		oid = Integer.parseInt(str[0]);
		t = Integer.parseInt(str[1]);
		lati = Double.parseDouble(str[2]);
		longi = Double.parseDouble(str[3]);
		if(currentTime==-1){
			currentTime=t;
		}
		else if(t>currentTime){
			if(clusterInput.size()>=m){
//				List<Cluster<PointWrapper>> clusterResults = dbscan.cluster(clusterInput);
				DBSCANNlogN algo = new DBSCANNlogN();  
				List<ca.pfv.spmf.patterns.cluster.Cluster> clusterResults = algo.runAlgorithmOnArray(m, e, clusterInput);
				//run pccdnode
				vcodaNode.PCCDNodeNLogN(clusterResults, k, m, currentTime);
				clusterInput.clear();
			}
			else{
				vcodaNode.PCCDNodeNLogN(null, k, m, currentTime);
				clusterInput.clear();
			}
		}
		
//		PointWrapper p = new PointWrapper(oid,longi,lati,t);
		double [] vector = new double[4]; 
		vector[0]=longi;
		vector[1]=lati;
		vector[2]=oid;
		vector[3]=t;
		clusterInput.add(new MyDoubleArrayDBS(vector));
//		clusterInput.add(p);
		
		currentTime=t;
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		//for last cluster
		if(clusterInput.size()>=m){
			DBSCANNlogN algo = new DBSCANNlogN();  
			List<ca.pfv.spmf.patterns.cluster.Cluster> clusterResults = algo.runAlgorithmOnArray(m, e, clusterInput);;
//			System.out.println("clustering done");
			vcodaNode.PCCDNodeNLogN(clusterResults, k, m, currentTime);
			clusterInput.clear();
		}
		else{
			vcodaNode.PCCDNodeNLogN(null, k, m, currentTime);
			clusterInput.clear();
		}
		Vpcc = vcodaNode.finishAlgo(currentTime);
		//*****************Apply PCCDNode algo on the list of clusters**************************************
		for(Convoy v:Vpcc){
			context.write(one, new Text(v.isLeftOpen()+","+v.isRightOpen()+
					","+v.getStartTime()+","+v.getEndTime()+
					","+v.getObjs().toString().replace("[", "").replace("]", "").replace(" ", "")));
		}
//		System.out.println("Convoys out from partition ("+minTime+"-"+maxTime+")"+Vpcc.size());
		super.cleanup(context);
		long end = context.getCounter(ConvoyCounters.MAP_END).getValue();
		if(end==0 || System.currentTimeMillis()>end){
			context.getCounter(ConvoyCounters.MAP_END).setValue(System.currentTimeMillis());
			context.getCounter(ConvoyCounters.MAP_PHASE).setValue(context.getCounter(ConvoyCounters.MAP_END).getValue() - context.getCounter(ConvoyCounters.MAP_START).getValue());
		}
	}
	
	
}