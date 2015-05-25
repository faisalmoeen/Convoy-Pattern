package clustering;

import org.apache.commons.math3.ml.clustering.Clusterable;



public class PointWrapper implements Clusterable{

	private int oid;
	private double[] point = new double[]{0,0};
	private long time;
	
	public PointWrapper() {
	}
	
	public PointWrapper(int oid,double x,double y, long time) {
		this.oid=oid;
		this.point[0]=x;
		this.point[1]=y;
		this.time = time;
	}

	@Override
	public double[] getPoint() {
		return point;
	}
	
	
	public int getOid() {
		return oid;
	}

	public void setOid(int oid) {
		this.oid = oid;
	}

	public double getX() {
		return point[0];
	}

	public void setX(double x) {
		this.point[0] = x;
	}

	public double getY() {
		return point[1];
	}

	public void setY(double y) {
		this.point[1] = y;
	}
	
	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}


	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.oid+":"+this.point[0]+","+this.point[1];
	}

	
	

}
