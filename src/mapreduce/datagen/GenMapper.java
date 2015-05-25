package mapreduce.datagen;
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


public class GenMapper
extends Mapper<Object, Text, PartitionTimePair, Text>{

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private int oid;
	private double lati;
	private double longi;

	int prevTime=-1;
	int currentTime=-1;
	int t=0;
	private int nr;
	private int np;
	private int ts;
	private double xs;
	private double ys;
	private static String[] str;
	private static int copiesPerPartition;
	private List<Tuple> tupleList;
	private static Tuple tuple;
	List<PointWrapper> clusterInput;
	DBSCANClusterer<PointWrapper> dbscan;
	HashMap<Integer,List<Cluster<PointWrapper>>> clusterMap;
	int count=0;


	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		
		nr = Integer.parseInt(conf.get("nr"));
		np = Integer.parseInt(conf.get("np"));
		ts = Integer.parseInt(conf.get("ts"));
		xs = Double.parseDouble(conf.get("xs"));
		ys = Double.parseDouble(conf.get("ys"));
		copiesPerPartition = (int) Math.ceil(nr/np);
		tupleList = new ArrayList<Tuple>();
		super.setup(context);
	}
	
	private int minTime=0;
	private int maxTime=0;
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		str = value.toString().split(",");
		tuple = new Tuple();
		tuple.oid = Integer.parseInt(str[0]);
		tuple.t = Integer.parseInt(str[1]);
		tuple.lati = Double.parseDouble(str[2]);
		tuple.longi = Double.parseDouble(str[3]);
		tupleList.add(tuple);
		if(tuple.t<minTime){
			minTime = tuple.t;
		}
		if(tuple.t>maxTime){
			maxTime = tuple.t;
		}
//		System.out.println(count++);
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		int startTime=minTime;
		int endTime=maxTime+(ts*nr);
		int timeSpan = (int) Math.ceil((endTime-startTime)/np);
		int timeShift=0;
		double xShift=0;
		double yShift=0;
		int partition=0;
		int oid=0;
		for(int replication=1;replication<=nr;replication++){
			for (Tuple tuple:tupleList){
				try {
					tuple.t+=timeShift;
					tuple.lati+=yShift;
					tuple.longi+=xShift;
					tuple.oid+=oid;
					partition = tuple.t/timeSpan;
//					System.out.println(partition+":"+tuple.t);
					context.write(new PartitionTimePair(partition, tuple.t), new Text(tuple.toString()));
				} catch (Exception e) {
					e.printStackTrace();
				}		
			}
			timeShift=ts;
			xShift=xs;
			yShift=ys;
			oid=300;
		}
		super.cleanup(context);
	}
	
	
}