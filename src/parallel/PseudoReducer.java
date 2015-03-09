package parallel;

import java.util.ArrayList;
import java.util.List;

import base.Convoy;

public class PseudoReducer {
	List<Convoy> VpccLeft = new ArrayList<Convoy>();
	List<Convoy> VpccRight = new ArrayList<Convoy>();
	List<Convoy> VpccResult = new ArrayList<Convoy>();
	public List<Convoy> getVpccResult() {
		return VpccResult;
	}

	public void setVpccResult(List<Convoy> vpccResult) {
		VpccResult = vpccResult;
	}

	int currentMergeTime=-1;
	int mergeTime;
	int count; //counter for comparisons
	int m;
	int k;
	
	public PseudoReducer(int m,int k) {
		this.m=m;
		this.k=k;
	}
	
	public void reduce(Convoy v){
		if(v.isClosed()){
			VpccResult.add(v);
			return;
		}
		mergeTime = getMergeTime(v);
		if(currentMergeTime==-1){
			currentMergeTime = mergeTime;
		}
		if(mergeTime!=currentMergeTime){
			performMerge(); //merge VpccLeft and VpccRight and add closed convoys to VpccResult
			currentMergeTime = mergeTime;
		}
		
		if(v.isLeftOpen()){ //if the convoy is bothOpen, it will also go to the right //VpccLeft will only contain convoys that are leftClosed
			VpccRight.add(v);
		}
		else{
			VpccLeft.add(v);
		}
		//******************Here the assumption is that both the lists are sorted************************
	}
	
	public List<Convoy> finalMerge(){
		performMerge();
		System.out.println("Total comparisons = "+count);
		return VpccResult;
	}
	
	private void performMerge(){
		List<Convoy> VpccNextLeft = new ArrayList<Convoy>();
		List<Convoy> VpccNextRight = new ArrayList<Convoy>();
		for(int i=0; i<VpccLeft.size(); i++){count++;
			Convoy v1 = VpccLeft.get(i);
//			System.out.println("i="+i);
			for(int j=0; j<VpccRight.size(); j++){count++;
				Convoy v2 = VpccRight.get(j);
				if(v1.intersection(v2).size()>=m){
					//merge two convoys
					v1.setExtended(true);
					Convoy vext = new Convoy(v1.intersection(v2),v1.getStartTime(),v2.getEndTime());
					vext.setLeftOpen(v1.isLeftOpen());vext.setRightOpen(v2.isRightOpen());
					if(vext.isOpen()){
						if(vext.isLeftOpen()){
							VpccNextRight = updateVpccResult(VpccNextRight, vext);
							System.out.println("this should never happen");
						}
						if(vext.isRightOpen()){
							VpccNextLeft = updateVpccResult(VpccNextLeft, vext);
						}
					}
					else if(vext.lifetime()>=k){
						VpccResult = updateVpccResult(VpccResult, vext);
					}
					if(v1.isSubset(v2)){v1.setAbsorbed(true);}
				}
			}
			if(!v1.isAbsorbed() && v1.lifetime() >= k){
				v1.setClosed();
				VpccResult = updateVpccResult(VpccResult, v1);
			}
		}
		VpccLeft = VpccNextLeft;
		VpccRight = VpccNextRight;
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
	
	private int getMergeTime(Convoy v){
		if(v.isLeftOpen()){
			return v.getStartTime()-1;
		}
		else if(v.isRightOpen()){
			return v.getEndTime();
		}
		else
			return -1;
	}
}
