package base;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.hamcrest.core.IsInstanceOf;

import clustering.PointWrapper;


public class Convoy {

	public Convoy() {
	}
	
	private List<Integer> objs = new ArrayList<Integer>();
	private int startTime;
	private int endTime;
	private boolean extended=false;
	private boolean absorbed=false;
	private boolean matched=false;
	private boolean rightOpen=false;
	private boolean leftOpen=false;
	
	public int lifetime(){
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
		Collections.sort(convoy.getObjs());
		return convoy;
	}
	
	public Convoy(List<Integer> objs, int startTime, int endTime){
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
	public void setTime(int t){
		this.startTime = t;
		this.endTime = t;
	}
	public List<Integer> getObjs() {
		return objs;
	}
	public void setObjs(List<Integer> objs) {
		this.objs = objs;
	}
	public int getStartTime() {
		return startTime;
	}
	public void setStartTime(int startTime) {
		this.startTime = startTime;
	}
	public int getEndTime() {
		return endTime;
	}
	public void setEndTime(int endTime) {
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
