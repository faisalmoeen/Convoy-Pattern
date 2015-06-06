package mapreduce;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.Utils;
import clustering.DbscanFile;
import clustering.PointWrapper;
import base.Convoy;
import base.Vcoda;

public class ConvoyReducer extends Reducer<IntWritable,Text,NullWritable,Text> {

	private IntWritable one = new IntWritable(1);
	private int m;
	private int k;
	private double e = 0.0006;
	private Convoy v;
	private int closedConvoyCount = 0;
	private String outputDir;

	static String inputFilePathVcoda="/home/faisal/Downloads/input/trucks273s.txt";
	static String outputFilePathVcoda="/home/faisal/Downloads/input/convoysOutput.txt";
	
	List<Convoy> VpccVcoda;
	List<Convoy> VpccMerge = new ArrayList<Convoy>();
	List<Convoy> VpccClosed = new ArrayList<Convoy>();
	
	int count=0;
	NullWritable nullwritable = NullWritable.get();
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		context.getCounter(ConvoyCounters.REDUCE_START).setValue(System.currentTimeMillis());
		Configuration conf = context.getConfiguration();
		m = Integer.parseInt(conf.get("m"));
		k = Integer.parseInt(conf.get("k"));
		outputDir = conf.get("output.dir");
		super.setup(context);
	}

	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		for (Text val : values) {
//			System.out.println(val);
			v = new Convoy(val.toString().split(","));
			if(v.isClosed()){
				context.write(nullwritable, new Text(v.toString()));
				closedConvoyCount++;
				VpccClosed.add(v);
			}
			else{
				VpccMerge.add(v);
			}
			count++;
		}
		System.out.println("Convoys received at reducer = "+count);
		//************
//		List<Convoy> VpccVcoda = runVcoda();
//		filterClosedConvoysinMerge(VpccMerge, VpccVcoda);
		//**************
		System.out.println("After removing closed convoys = "+VpccMerge.size());
		VpccMerge = finalMerge(VpccMerge, 1);
		for(Convoy v:VpccMerge){
			context.write(nullwritable, new Text(v.toString()));
			closedConvoyCount++;
		}
		//**************
//		PrintWriter pw = new PrintWriter(new File(outputDir+"/stats.txt"));
//		pw.println("Convoys received at reducer = "+count);
//		pw.println("Total discovered convoys : " + closedConvoyCount);
//		pw.flush();
//		pw.close();
//		System.out.println("After final merge = "+VpccMerge.size());
//		System.out.println("closed convoys");
//		printConvoyList(VpccClosed);
//		filterClosedConvoysinMerge(VpccMerge, VpccVcoda);
////		filterClosedConvoysinMerge(VpccClosed, VpccVcoda);
//		System.out.println("After filtering = "+VpccMerge.size());
//		System.out.println("**************Vpccn Remaining Convoys*************");
//		printConvoyList(VpccMerge);
//		System.out.println("**************Vpcc Remaining Convoys*************");
//		printConvoyList(VpccVcoda);
		//*******************
		System.out.println("Total discovered convoys : " + closedConvoyCount);
		System.out.println("**************End*************");
//		System.out.println("Total comparison ops during merge = "+count);
	}
	
	private List<Convoy> finalMerge(List<Convoy> Vpcc,int iteration){
		List<Convoy> VpccResult = new ArrayList<Convoy>();
		List<Convoy> VpccNext = new ArrayList<Convoy>();
		System.out.println("Iteration#"+iteration+", input convoys = "+Vpcc.size());
		for(int i=0; i<Vpcc.size(); i++){
			Convoy v1 = Vpcc.get(i);
//			System.out.println("i="+i);
			for(int j=i+1; j<Vpcc.size(); j++){count++;
				Convoy v2 = Vpcc.get(j);
				if((v1.getStartTime()==185 && v1.getEndTime()==500) || (v1.getStartTime()==501 && v1.getEndTime()==836)){// && v1.getEndTime()==1600){
//					System.out.println("i="+i+"::j="+j);
				}
				if(v1.isRightOpen()){
					if( v2.getStartTime()<=v1.getEndTime()+1 && v1.getEndTime()<v2.getEndTime() && v1.intersection(v2).size()>=m){
						//merge two convoys
						v1.setExtended(true);
						Convoy vext = new Convoy(v1.intersection(v2),v1.getStartTime(),v2.getEndTime());
						vext.setLeftOpen(v1.isLeftOpen());vext.setRightOpen(v2.isRightOpen());
						if(vext.isOpen()){
							VpccNext = updateVpccResult(VpccNext, vext);
						}
						else if(vext.lifetime()>=k){
							VpccResult = updateVpccResult(VpccResult, vext);
						}
						if(v1.isSubset(v2)){v1.setAbsorbed(true);}
					}
				}
				if(v1.isLeftOpen()){
					if( v2.getStartTime()<v1.getStartTime() && v1.getStartTime()<=v2.getEndTime()+1 && v1.intersection(v2).size()>=m){
						//merge two convoys
//						System.out.println("Came to left open");
						Convoy vext = new Convoy(v1.intersection(v2),v2.getStartTime(),v1.getEndTime());
						vext.setLeftOpen(v2.isLeftOpen());vext.setRightOpen(v1.isRightOpen());
						if(vext.isOpen()){
							VpccNext = updateVpccResult(VpccNext, vext);
						}
						else if(vext.lifetime()>=k){
							VpccResult = updateVpccResult(VpccResult, vext);
						}
						if(v1.isSubset(v2)){v1.setAbsorbed(true);}
					}
				}
			}
			if(!v1.isAbsorbed() && v1.lifetime() >= k){
				v1.setClosed();
				VpccResult = updateVpccResult(VpccResult, v1);
			}
		}
		if(VpccNext.size()>0){
			VpccNext = finalMerge(VpccNext,++iteration);
			for(Convoy v:VpccNext){
				VpccResult = updateVpccResult(VpccResult,v);
			}
		}
		return VpccResult;
	}
	
	public static List<Convoy> updateVpccResult(List<Convoy> VpccResult, Convoy vnew){
		boolean added=false;
		List<Convoy> toRemove=new ArrayList<Convoy>();
		List<Convoy> toAdd=new ArrayList<Convoy>();
		for(Convoy v : VpccResult){
			if(v.hasSameObjs(vnew)){
				if(v.getStartTime()>=vnew.getStartTime()
						&& v.getEndTime()<=vnew.getEndTime()){//v is a subconvoy of vnew
					toRemove.add(v);
					if(!added){
						toAdd.add(vnew);
						added=true;
					}
				}
				else if(vnew.getStartTime()>=v.getStartTime()
						&& vnew.getEndTime()<=v.getEndTime()){//vnew is a subconvoy of v *****different from vcoda
					added=true;
				}
			}
		}
		if(added==false){
			VpccResult.add(vnew);
		}
		for(Convoy v:toRemove){VpccResult.remove(v);}
		for(Convoy v:toAdd){VpccResult.add(v);}
		
		return VpccResult;
	}
	
	
	public List<Convoy> runVcoda() throws IOException{
		HashMap<Integer,List<Cluster<PointWrapper>>> clusterMap = new DbscanFile().DBSCAN(inputFilePathVcoda, m-1, e, 1);

		//*****************Apply PCCD algo on the list of clusters**************************************
		List<Convoy> Vpcc = Vcoda.PCCD(clusterMap,k,m);
		//*******Print Vpcc************
		Utils.writeConvoys(Vpcc, outputFilePathVcoda);
		return Vpcc;
	}
	
	public void filterClosedConvoysinMerge(List<Convoy> VpccResult, List<Convoy> VpccTrue){
		//*********filter out closed convoys**********//
				List<Convoy> toRemove=new ArrayList<Convoy>();
				for(Convoy v:VpccTrue){
					if(VpccResult.contains(v)){
						toRemove.add(v);
					}
				}
				System.out.println("To Remove size = " + toRemove.size());
				int sizeVpcc=VpccTrue.size();
				System.out.println("Vpcc Size before Filtering = "+sizeVpcc);
				int sizeVpccn=VpccResult.size();
				System.out.println("Vpccn Size before Filtering = "+sizeVpccn);
				for(Convoy v:toRemove){
					sizeVpcc=VpccTrue.size();
					sizeVpccn=VpccResult.size();
					VpccResult.remove(v);
					VpccTrue.remove(v);
					if(VpccTrue.size()-sizeVpcc>1){
						System.out.println("The convoy exists more in Vpcc true");
						System.out.println(v);
					}
					if(VpccResult.size()-sizeVpccn>1){
						System.out.println("The convoy exists more in Vpccn Result");
						System.out.println(v);
					}
				}
				System.out.println("Vpcc Size after Filtering = "+VpccTrue.size());
				System.out.println("Vpccn Size after Filtering = "+VpccResult.size());
				System.out.println("Convoys closed = "+(VpccResult.size()-VpccTrue.size()));
				this.VpccVcoda=VpccTrue;
				this.VpccMerge=VpccResult;
	}
	
	private void printConvoyList(List<Convoy> Vpcc){
		for(Convoy v:Vpcc){
			System.out.println(v);
		}
	}

	@Override
	protected void cleanup(
			Reducer<IntWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
		context.getCounter(ConvoyCounters.REDUCE_END).setValue(System.currentTimeMillis());
		context.getCounter(ConvoyCounters.REDUCE_PHASE).setValue(context.getCounter(ConvoyCounters.REDUCE_END).getValue() - context.getCounter(ConvoyCounters.REDUCE_START).getValue());
		context.getCounter(ConvoyCounters.CONVOY_RESULTS).setValue(VpccMerge.size());
	}
	
	

}