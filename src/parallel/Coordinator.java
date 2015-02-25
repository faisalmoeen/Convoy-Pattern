package parallel;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.math3.ml.clustering.Cluster;
import org.junit.Test;

import utils.Utils;
import base.Convoy;
import base.Vcoda;
import clustering.DbscanFile;
import clustering.PointWrapper;

public class Coordinator {

	static String inputFilePathVcoda="C:/Users/buraq/Google Drive/Brussels/PhD Work/working-folder/experiments/trucks_dataset/trucks273s.txt";
	static String outputFilePathVcoda="C:/Users/buraq/Google Drive/Brussels/PhD Work/working-folder/experiments/trucks_dataset/convoysOutput.txt";
	static String inputFolder="C:/Users/buraq/Google Drive/Brussels/PhD Work/working-folder/experiments/trucks_dataset/partitions";
	static String outputFolder="C:/Users/buraq/Google Drive/Brussels/PhD Work/working-folder/experiments/trucks_dataset/partitions_output";
	static int m=3;
	static double e=0.0006; // Range: 1/10^4 to 6/10^4
	static int k=180;
//	static int partitionSize=500;
	static List<List<Convoy>> VpccList=null;
	static List<Convoy> Vpcc = null;
	static List<Convoy> VpccMerge = new ArrayList<Convoy>();
	int count=0;
	public Coordinator() {
		// TODO Auto-generated constructor stub
	}
	
	@Test
	public void testParallelVcoda() throws IOException{
		VpccList = runParallelVcoda();
		
		for(List<Convoy> V:VpccList){
			for(Convoy v:V){
				VpccMerge.add(v);
			}
		}
		
		System.out.println("No. of convoys before merge = "+VpccMerge.size());
		VpccMerge = newMerge(VpccMerge,1);
		System.out.println("Convoys Count = "+VpccMerge.size());
		System.out.println("Total comparison ops = "+ count);
	}
	
	private List<Convoy> newMerge(List<Convoy> Vpcc,int iteration){
		List<Convoy> VpccResult = new ArrayList<Convoy>();
		List<Convoy> VpccNext = new ArrayList<Convoy>();
		System.out.println("Iteration#"+iteration+", input convoys = "+Vpcc.size());
		
		List<Convoy> VpccLeft = new ArrayList<Convoy>();
		List<Convoy> VpccRight = new ArrayList<Convoy>();
		
		for(Convoy v:Vpcc){
			if(v.isLeftOpen()){
				VpccRight.add(v);
			}
			else if(v.isRightOpen()){
				VpccLeft.add(v);
			}
			else{
				VpccResult.add(v);
			}
		}
		//******************Here the assumption is that both the lists are sorted************************
		
		for(int i=0; i<Vpcc.size(); i++){
			Convoy v1 = Vpcc.get(i);
//			System.out.println("i="+i);
			for(int j=i+1; j<Vpcc.size(); j++){count++;
				Convoy v2 = Vpcc.get(j);
//				if(v1.getStartTime()==1337 && v1.getEndTime()==1600){
//					System.out.println("i="+i+"::j="+j);
//				}
				if(v1.isRightOpen()){
					if( v2.getStartTime()<=v1.getEndTime()+1 && v1.getEndTime()<v2.getEndTime() && v1.intersection(v2).size()>=m){
						//merge two convoys
						v1.setExtended(true);
						Convoy vext = new Convoy(v1.intersection(v2),v1.getStartTime(),v2.getEndTime());
						vext.setLeftOpen(v1.isLeftOpen());vext.setRightOpen(v2.isRightOpen());
						if(vext.isLeftOpen() || vext.isRightOpen()){
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
	
	@Test
	public void compareResults() throws IOException{
		VpccList = runParallelVcoda();
		Vpcc = runVcoda();
//		VpccList = closeStartEndConvoys(VpccList);
		filterClosedConvoys(VpccList, Vpcc);
		System.out.println("Phase#1");
//		VpccMerge = mergeGlobal(VpccList);
		for(List<Convoy> V:VpccList){
			for(Convoy v:V){
				VpccMerge.add(v);
			}
		}
//		System.out.println("Phase#2 Global Merge");
//		filterClosedConvoysinMerge(VpccMerge, Vpcc);
		System.out.println("Phase#3 Final Merge");
		VpccMerge = finalMerge(VpccMerge,1);
		filterClosedConvoysinMerge(VpccMerge, Vpcc);
		System.out.println("**************Vpccn Remaining Convoys*************");
		printConvoyList(VpccMerge);
		System.out.println("**************Vpcc Remaining Convoys*************");
		printConvoyList(Vpcc);
		System.out.println("**************End*************");
		System.out.println("Total comparison ops = "+ count);
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
//				if(v1.getStartTime()==1337 && v1.getEndTime()==1600){
//					System.out.println("i="+i+"::j="+j);
//				}
				if(v1.isRightOpen()){
					if( v2.getStartTime()<=v1.getEndTime()+1 && v1.getEndTime()<v2.getEndTime() && v1.intersection(v2).size()>=m){
						//merge two convoys
						v1.setExtended(true);
						Convoy vext = new Convoy(v1.intersection(v2),v1.getStartTime(),v2.getEndTime());
						vext.setLeftOpen(v1.isLeftOpen());vext.setRightOpen(v2.isRightOpen());
						if(vext.isLeftOpen() || vext.isRightOpen()){
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
	private List<Convoy> mergeGlobal(List<List<Convoy>> VpccList){
		List<Convoy> VpccLeft=null;
		List<Convoy> VpccRight=null;
		List<Convoy> VpccResult = new ArrayList<Convoy>();
		for(int i=0;i<VpccList.size()-1;i++){
			VpccLeft = VpccList.get(i);
			VpccRight =  VpccList.get(i+1);
			for(Convoy v1:VpccLeft){
				if(v1.isRightOpen()){
					for(Convoy v2:VpccRight){
						if(v2.isLeftOpen()){
							//********match the left and right convoys*********
							if(v1.intersection(v2).size() >= m &&
									((v2.getEndTime()-v1.getStartTime())>=(k-1) ||
									v1.isOpen() || v2.isOpen())){
								Convoy vext = new Convoy(v1.intersection(v2),v1.getStartTime(),v2.getEndTime());
								vext.setLeftOpen(v1.isLeftOpen());vext.setRightOpen(v2.isRightOpen());
								VpccResult = updateVpccResult(VpccResult, vext);
								if(v1.isSubset(v2)){v1.setAbsorbed(true);}
								if(v2.isSubset(v1)){v2.setAbsorbed(true);}
							}
						}
					}
					if(!v1.isAbsorbed()){
						if(v1.isLeftOpen() || v1.lifetime() >= k){ //if leftOpen, it should be added to the result so that in merging phase, complete closed convoy can be found
							v1.setRightOpen(false);
							VpccResult = updateVpccResult(VpccResult, v1);
						}
					}
				}
			}
		}
		for(Convoy v2 : VpccRight){
			if(v2.isLeftOpen()){
				if(!v2.isAbsorbed()){
					if(v2.isRightOpen() || v2.lifetime() >=k){
						v2.setLeftOpen(false);
						VpccResult = updateVpccResult(VpccResult, v2);
					}
				}
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
	private void printConvoys(List<List<Convoy>> VpccList){
		if(VpccList.size()==0){
			System.out.println("nothing to print");
		}
		for(int i=0;i<VpccList.size();i++){
//			VpccLeft = VpccList.get(i);
//			VpccRight =  VpccList.get(i+1);
			List<Convoy> Vpcc = VpccList.get(i);
			for(Convoy v:Vpcc){
				if(!v.isLeftOpen() || !v.isRightOpen()){
					System.out.println("Closed = " + v);
				}
			}
		}
	}
	private void printConvoyList(List<Convoy> Vpcc){
		for(Convoy v:Vpcc){
			System.out.println(v);
		}
	}
	private List<List<Convoy>> closeStartEndConvoys(List<List<Convoy>> VpccList){
		List<Convoy> VpccStart = VpccList.get(0);
		List<Convoy> VpccEnd = VpccList.get(VpccList.size()-1);
		for(Convoy v:VpccStart){
			if(v.isLeftOpen()){
				v.setLeftOpen(false);
			}
		}
		for(Convoy v:VpccEnd){
			if(v.isRightOpen()){
				v.setRightOpen(false);
			}
		}
		return VpccList;
	}
	public List<List<Convoy>> runParallelVcoda() throws IOException{
		File f= new File(inputFolder);
		List<List<Convoy>> VpccList = new ArrayList<List<Convoy>>();
		for(String file:f.list()){
			String inputFilePath = inputFolder+"/"+file;
			HashMap<Integer,List<Cluster<PointWrapper>>> clusterMap = new DbscanFile().DBSCAN(inputFilePath, m-1, e,1);
			
			//*****************Apply PCCDNode algo on the list of clusters**************************************
			if(clusterMap.size()==0){
				continue;
			}
			System.out.println(clusterMap.size());
			int minTime=Collections.min(clusterMap.keySet());
			int maxTime=Collections.max(clusterMap.keySet());
			List<Convoy> Vpcc = VcodaNode.PCCDNode(clusterMap,k,m,minTime,maxTime,1,2874);
			VpccList.add(Vpcc);
			
			//*******Print Vpcc************
			String outputFilePath=outputFolder+"/out"+file;
			Utils.writeConvoys(Vpcc, outputFilePath);
			System.out.println(file);
		}
		return VpccList;
	}
	
	public void filterClosedConvoys(List<List<Convoy>> VpccList, List<Convoy> Vpcc){
		//*********filter out closed convoys**********//
				List<Convoy> toRemove=new ArrayList<Convoy>();
				for(List<Convoy> Vpccn:VpccList){
					for(Convoy v:Vpccn){
						if(Vpcc.contains(v)){
							toRemove.add(v);
						}
					}
				}
				int size1=Vpcc.size();
				System.out.println("Vpcc Size before Filtering = "+size1);
				int size=0;
				for(List<Convoy> Vpccn:VpccList){
					size+=Vpccn.size();
				}
				System.out.println("Vpccn Size before Filtering = "+size);
				for(Convoy v:toRemove){
					Vpcc.remove(v);
					for(List<Convoy> Vpccn:VpccList){
						Vpccn.remove(v);
					}
				}
				size=0;
				for(List<Convoy> Vpccn:VpccList){
					size+=Vpccn.size();
				}
				System.out.println("Vpcc Size after Filtering = "+Vpcc.size());
				System.out.println("Vpccn Size after Filtering = "+size);
				System.out.println("Convoys closed = "+(size1-Vpcc.size()));
				this.Vpcc=Vpcc;
				this.VpccList=this.VpccList;
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
				this.Vpcc=VpccTrue;
				this.VpccMerge=VpccResult;
	}
	
	public List<Convoy> runVcoda() throws IOException{
		HashMap<Integer,List<Cluster<PointWrapper>>> clusterMap = new DbscanFile().DBSCAN(inputFilePathVcoda, m-1, e,1);

		//*****************Apply PCCD algo on the list of clusters**************************************
		List<Convoy> Vpcc = Vcoda.PCCD(clusterMap,k,m);
		//*******Print Vpcc************
		Utils.writeConvoys(Vpcc, outputFilePathVcoda);
		return Vpcc;
	}

}
