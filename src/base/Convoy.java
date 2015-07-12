package base;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.hamcrest.core.IsInstanceOf;

import utils.DBSCAN.Trajectory;
import ca.pfv.spmf.patterns.cluster.DoubleArray;
import clustering.PointWrapper;


public class Convoy {

	public Convoy() {
	}
	
	private List<Integer> objs = new ArrayList<Integer>();
	private long startTime;
	private long endTime;
	private boolean extended=false;
	private boolean absorbed=false;
	private boolean matched=false;
	private boolean rightOpen=false;
	private boolean leftOpen=false;
	
	public long lifetime(){
		return endTime-startTime+1;
	}
	
	public int size(){
		return objs.size();
	}
	
	public boolean hasSameObjs(Convoy v){
		List<Integer> objs2 = v.getObjs();
		if(objs.size()==objs2.size() && objs.containsAll(objs2) && objs2.containsAll(objs)){
			return true;
		}
		else
			return false;
	}
	public boolean isSubset(Convoy v){
		if(v.getObjs().containsAll(objs))
			return true;
		else
			return false;
	}
	public List<Integer> intersection(Convoy v){
		int count=0;
		List<Integer> objs2 = v.getObjs();
		List<Integer> intersection=new ArrayList<Integer>();
		for(int obj : objs){
			if(objs2.contains(obj)){
				intersection.add(obj);
			}
		}
		return intersection;
	}
	public void addObject(int oid){
		this.objs.add(oid);
	}
	public static Convoy createConvoy(List<PointWrapper> pts){
		Convoy convoy = new Convoy();
		for(PointWrapper p : pts){
			convoy.addObject(p.getOid());
		}
		convoy.setTime(pts.get(0).getTime());
		Collections.sort(convoy.getObjs());
		return convoy;
	}
	public static Convoy createConvoyFromTraj(List<Trajectory> pts){
		Convoy convoy = new Convoy();
		for(Trajectory p : pts){
			convoy.addObject(p.getOid());
		}
		convoy.setStartTime(pts.get(0).getStart());
		convoy.setEndTime(pts.get(pts.size()-1).getEnd());
		Collections.sort(convoy.getObjs());
		return convoy;
	}
	
	
	public static Convoy createConvoyFromDArray(List<DoubleArray> vectors){
		Convoy convoy = new Convoy();
		for(DoubleArray vector:vectors){
			double[] data=vector.data;
			convoy.addObject((int)data[2]);
			convoy.setTime((int)data[3]);
		}
		Collections.sort(convoy.getObjs());
		return convoy;
	}
	
	public Convoy(List<Integer> objs, long startTime, long endTime){
		this.objs=objs;
		this.startTime=startTime;
		this.endTime=endTime;
	}
	
	public Convoy(String[] c){
		this.leftOpen=Boolean.parseBoolean(c[0]);
		this.rightOpen=Boolean.parseBoolean(c[1]);
		this.startTime = Integer.parseInt(c[2]);
		this.endTime = Integer.parseInt(c[3]);
		for(int i=4;i<c.length;i++){
			this.objs.add(Integer.parseInt(c[i]));
		}
	}
	public void setTime(long l){
		this.startTime = l;
		this.endTime = l;
	}
	public List<Integer> getObjs() {
		return objs;
	}
	public void setObjs(List<Integer> objs) {
		this.objs = objs;
	}
	public long getStartTime() {
		return startTime;
	}
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
	public long getEndTime() {
		return endTime;
	}
	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public boolean isExtended() {
		return extended;
	}

	public void setExtended(boolean extended) {
		this.extended = extended;
	}

	public boolean isAbsorbed() {
		return absorbed;
	}

	public void setAbsorbed(boolean absorbed) {
		this.absorbed = absorbed;
	}

	public boolean isMatched() {
		return matched;
	}

	public void setMatched(boolean matched) {
		this.matched = matched;
	}

	public String toString(){
		String s = "life = ["+startTime+"  "+endTime+"], objs = [";
		int i=0;
		for(int obj : objs){
			if(i==0){
				s = s + obj;
			}
			else{
				s = s+"  "+obj;
			}
			i++;
		}
		return s+"] :: leftOpen = "+leftOpen+" rightOpen = "+rightOpen+"\n";
	}

	public boolean isRightOpen() {
		return rightOpen;
	}

	public void setRightOpen(boolean rightOpen) {
		this.rightOpen = rightOpen;
	}

	public boolean isLeftOpen() {
		return leftOpen;
	}

	public void setLeftOpen(boolean leftOpen) {
		this.leftOpen = leftOpen;
	}
	
	public boolean isOpen(){
		return leftOpen || rightOpen;
	}
	
	public boolean isClosed(){
		return !isOpen();
	}
	
	public void setClosed(){
		leftOpen=false;
		rightOpen=false;
	}
	
	public long getMergeTime(){
		if(isLeftOpen()){
			return startTime-1;
		}
		else if(isRightOpen()){
			return endTime;
		}
		else
			return -1;
	}
	

	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof Convoy))
			return false;
		Convoy c= (Convoy)obj;
		if(c.getStartTime()==startTime &&
				c.getEndTime()==endTime &&
				c.getObjs().equals(objs) &&
//				c.isAbsorbed()==absorbed &&
//				c.isExtended()==extended &&
				c.isLeftOpen()==leftOpen &&
//				c.isMatched()==matched &&
				c.isRightOpen()==rightOpen){
			return true;
		}
		return false;
	}
	
	

}
