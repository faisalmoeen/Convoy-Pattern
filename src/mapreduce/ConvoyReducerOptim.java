package mapreduce;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import parallel.PseudoReducer;
import utils.Utils;
import clustering.DbscanFileReader;
import clustering.PointWrapper;
import base.Convoy;
import base.Vcoda;

public class ConvoyReducerOptim extends Reducer<IntWritable,Text,IntWritable,Text> {

	private IntWritable one = new IntWritable(1);
	private int m;
	private int k;
	private double e = 0.0006;
	private Convoy v;
	private PseudoReducer reducer;
	private List<Convoy> result=null;
	static String inputFilePathVcoda="/media/mynewdrive/research-data/vcoda-inputs/trucks273s.txt";
	static String outputFilePathVcoda="/media/mynewdrive/research-data/vcoda-results/convoysOutput.txt";
	
	List<Convoy> VpccVcoda;
	List<Convoy> VpccMerge = new ArrayList<Convoy>();
	
	int reducerCount=0;
	int count=0;
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		m = Integer.parseInt(conf.get("m"));
		k = Integer.parseInt(conf.get("k"));
		reducer = new PseudoReducer(m, k);
		super.setup(context);
	}

	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		reducerCount++;
		int count = 0;
		for (Text val : values) {
			System.out.println(val);
			v = new Convoy(val.toString().split(","));
//			if(!v.isOpen()){
//				context.write(val, one);
//			}
//			else{
				reducer.reduce(v);
				count++;
//			}
		}
		System.out.println("Convoys received at reducer = "+count);
//		List<Convoy> VpccVcoda = runVcoda();
//		filterClosedConvoysinMerge(VpccMerge, VpccVcoda);
//		System.out.println("After removing closed convoys = "+VpccMerge.size());
//		VpccMerge = finalMerge(VpccMerge, 1);
//		System.out.println("After final merge = "+VpccMerge.size());
//		filterClosedConvoysinMerge(VpccMerge, VpccVcoda);
//		System.out.println("After filtering = "+VpccMerge.size());
//		System.out.println("**************Vpccn Remaining Convoys*************");
//		printConvoyList(VpccMerge);
//		System.out.println("**************Vpcc Remaining Convoys*************");
//		printConvoyList(VpccVcoda);
		System.out.println("**************End*************");
		System.out.println("Total comparison ops during merge = "+count);
	}
	
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		reducer.finalMerge();
		result=reducer.getVpccResult();
		int id=0;
		for(Convoy v:result){
			context.write(new IntWritable(id++), new Text(v.toString()));
		}
		System.out.println("Total convoys = "+result.size());
		System.out.println("Total reducer calls = "+reducerCount);
		List<Convoy> VpccVcoda = runVcoda();
		filterClosedConvoysinMerge(result, VpccVcoda);
		System.out.println("**************Vpccn Remaining Convoys*************");
		printConvoyList(VpccMerge);
		System.out.println("**************Vpcc Remaining Convoys*************");
		printConvoyList(VpccVcoda);
		super.cleanup(context);
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
					toAdd.add(vnew);
					added=true;
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
		HashMap<Integer,List<Cluster<PointWrapper>>> clusterMap = new DbscanFileReader().DBSCAN(inputFilePathVcoda, m-1, e, 1);

		//*****************Apply PCCD algo on the list of clusters**************************************
		List<Convoy> Vpcc = Vcoda.PCCD(clusterMap,k,m);
		//*******Print Vpcc************
		Utils.writeConvoys(Vpcc, outputFilePathVcoda);
		return Vpcc;
	}
	
	public void filterClosedConvoysinMerge(List<Convoy> VpccResult, List<Convoy> VpccTrue){
		//*********filter out closed convoys**********//
				List<Convoy> toRemove=new ArrayList<Convoy>();
				for(Convoy v:VpccResult){
					if(VpccTrue.contains(v)){
						toRemove.add(v);
					}
				}
				int sizeVpcc=VpccTrue.size();
				System.out.println("Vpcc Size before Filtering = "+sizeVpcc);
				int sizeVpccn=VpccResult.size();
				System.out.println("Vpccn Size before Filtering = "+sizeVpccn);
				for(Convoy v:toRemove){
					VpccResult.remove(v);
					VpccTrue.remove(v);
				}
				System.out.println("Vpcc Size after Filtering = "+VpccTrue.size());
				System.out.println("Vpccn Size after Filtering = "+VpccResult.size());
				System.out.println("Convoys closed = "+(sizeVpcc-VpccTrue.size()));
				this.VpccVcoda=VpccTrue;
				this.VpccMerge=VpccResult;
	}
	
	private void printConvoyList(List<Convoy> Vpcc){
		for(Convoy v:Vpcc){
			System.out.println(v);
		}
	}

}