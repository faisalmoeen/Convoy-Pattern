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

import utils.Utils;
import clustering.DbscanFile;
import clustering.PointWrapper;
import base.Convoy;
import base.Vcoda;

public class ConvoyReducer extends Reducer<IntWritable,Text,Text,IntWritable> {

	private IntWritable one = new IntWritable(1);
	private int m;
	private int k;
	private double e = 0.0006;
	private Convoy v;

	static String inputFilePathVcoda="/media/mynewdrive/research-data/vcoda-inputs/trucks273s.txt";
	static String outputFilePathVcoda="/media/mynewdrive/research-data/vcoda-results/convoysOutput.txt";
	
	List<Convoy> VpccVcoda;
	List<Convoy> VpccMerge = new ArrayList<Convoy>();
	
	int count=0;
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		m = Integer.parseInt(conf.get("m"));
		k = Integer.parseInt(conf.get("k"));
		super.setup(context);
	}

	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		for (Text val : values) {
			System.out.println(val);
			v = new Convoy(val.toString().split(","));
//			if(!v.isOpen()){
//				context.write(val, one);
//			}
//			else{
				VpccMerge.add(v);
				count++;
//			}
		}
		System.out.println("Convoys received at reducer = "+count);
		List<Convoy> VpccVcoda = runVcoda();
		filterClosedConvoysinMerge(VpccMerge, VpccVcoda);
		System.out.println("After removing closed convoys = "+VpccMerge.size());
		VpccMerge = finalMerge(VpccMerge, 1);
		System.out.println("After final merge = "+VpccMerge.size());
		filterClosedConvoysinMerge(VpccMerge, VpccVcoda);
		System.out.println("After filtering = "+VpccMerge.size());
		System.out.println("**************Vpccn Remaining Convoys*************");
		printConvoyList(VpccMerge);
		System.out.println("**************Vpcc Remaining Convoys*************");
		printConvoyList(VpccVcoda);
		System.out.println("**************End*************");
		System.out.println("Total comparison ops during merge = "+count);
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
					System.out.println("i="+i+"::j="+j);
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
						System.out.println("Came to left open");
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