package parallel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.math3.ml.clustering.Cluster;

import utils.Utils;
import ca.pfv.spmf.patterns.cluster.DoubleArray;
import clustering.PointWrapper;
import base.Convoy;


public class VcodaNode {

	public VcodaNode(){
		
	}
	
	public VcodaNode(int globalMinTime, int globalMaxTime) {
		
	}
	int minTime=Integer.MAX_VALUE, globalMinTime, globalMaxTime;
	List<Convoy> Vnext = new ArrayList<Convoy>();
	List<Convoy> V = new ArrayList<Convoy>();
	List<Convoy> Vpcc = new ArrayList<Convoy>();
	List<Convoy> C = null;
	
	public void PCCDNode(List<Cluster<PointWrapper>> CC, int k, int m, int t){
		if(t<minTime){
			minTime=t;
		}
		Vnext=new ArrayList<Convoy>();
		if(CC!=null && CC.size()>0){
			C = Utils.clusterToConvoyList(CC);
		}
		else{
			for(Convoy v:V){
				if(v.lifetime()>=k){
					Vpcc.add(v);
				}
			}
			V=new ArrayList<Convoy>();
			return;
		}
		for(Convoy c : C){
			c.setMatched(false);c.setAbsorbed(false);c.setTime(t);
			if(t==minTime && t!=globalMinTime){
				c.setLeftOpen(true);
			}
		}
		for(Convoy v : V){
			v.setExtended(false);v.setAbsorbed(false);
			for(Convoy c : C){
				if(c.size()>=m && v.intersection(c).size() >= m){ //extend (v,c)
					v.setExtended(true);c.setMatched(true);
					Convoy vext = new Convoy(v.intersection(c),v.getStartTime(),t);
					vext.setLeftOpen(v.isLeftOpen());//inherit the leftOpen property
					Vnext = updateVnext(Vnext, vext);
					if(v.isSubset(c)){v.setAbsorbed(true);}
					if(c.isSubset(v)){c.setAbsorbed(true);}
				}
			}
			if(!v.isAbsorbed()){
				if(v.isLeftOpen() || v.lifetime() >= k){ //if leftOpen, it should be added to the result so that in merging phase, complete closed convoy can be found
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
	
	public void PCCDNodeNLogN(List<ca.pfv.spmf.patterns.cluster.Cluster> CC, int k, int m, int t){
		if(t<minTime){
			minTime=t;
		}
		Vnext=new ArrayList<Convoy>();
		if(CC!=null && CC.size()>0){
			C = Utils.clustersToConvoyList(CC);
		}
		else{
			for(Convoy v:V){
				if(v.lifetime()>=k){
					Vpcc.add(v);
				}
			}
			V=new ArrayList<Convoy>();
			return;
		}
		for(Convoy c : C){
			c.setMatched(false);c.setAbsorbed(false);c.setTime(t);
			if(t==minTime && t!=globalMinTime){
				c.setLeftOpen(true);
			}
		}
		for(Convoy v : V){
			v.setExtended(false);v.setAbsorbed(false);
			for(Convoy c : C){
				if(c.size()>=m && v.intersection(c).size() >= m){ //extend (v,c)
					v.setExtended(true);c.setMatched(true);
					Convoy vext = new Convoy(v.intersection(c),v.getStartTime(),t);
					vext.setLeftOpen(v.isLeftOpen());//inherit the leftOpen property
					Vnext = updateVnext(Vnext, vext);
					if(v.isSubset(c)){v.setAbsorbed(true);}
					if(c.isSubset(v)){c.setAbsorbed(true);}
				}
			}
			if(!v.isAbsorbed()){
				if(v.isLeftOpen() || v.lifetime() >= k){ //if leftOpen, it should be added to the result so that in merging phase, complete closed convoy can be found
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
	
	public List<Convoy> finishAlgo(int maxTime){
		//**********for last remaining convoys**************//
		for(Convoy v:V){
//			if(v.lifetime()>=k){
//				Vpcc.add(v);
//			}
			if(maxTime!=globalMaxTime){
				v.setRightOpen(true);
			}
			Vpcc.add(v);
		}
		return Vpcc;
	}
	
	
	public List<Convoy> PCCDNode(HashMap<Integer, List<Cluster<PointWrapper>>> clusterMap, int k, int m, int minTime, int maxTime,
			int globalMinTime, int globalMaxTime){
		System.out.println("minTime="+minTime+"::maxTime="+maxTime);
		List<Convoy> Vnext = new ArrayList<Convoy>();
		List<Convoy> V = new ArrayList<Convoy>();
		List<Convoy> Vpcc = new ArrayList<Convoy>();
		List<Convoy> C = null;
		for(int t=minTime;t<=maxTime;t++){//maxTime;t++){
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
				if(t==minTime && t!=globalMinTime){
					c.setLeftOpen(true);
				}
			}
			for(Convoy v : V){
				v.setExtended(false);v.setAbsorbed(false);
				for(Convoy c : C){
					if(c.size()>=m && v.intersection(c).size() >= m){ //extend (v,c)
						v.setExtended(true);c.setMatched(true);
						Convoy vext = new Convoy(v.intersection(c),v.getStartTime(),t);
						vext.setLeftOpen(v.isLeftOpen());//inherit the leftOpen property
						Vnext = updateVnext(Vnext, vext);
						if(v.isSubset(c)){v.setAbsorbed(true);}
						if(c.isSubset(v)){c.setAbsorbed(true);}
					}
				}
				if(!v.isAbsorbed()){
					if(v.isLeftOpen() || v.lifetime() >= k){ //if leftOpen, it should be added to the result so that in merging phase, complete closed convoy can be found
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
		//**********for last remaining convoys**************//
		for(Convoy v:V){
//			if(v.lifetime()>=k){
//				Vpcc.add(v);
//			}
			if(maxTime!=globalMaxTime){
				v.setRightOpen(true);
			}
			Vpcc.add(v);
		}
		return Vpcc;
	}
	
	//********************************************************************//
	public List<Convoy> updateVnext(List<Convoy> Vnext, Convoy vnew){
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
	
}
