package clustering;

import org.apache.commons.math3.ml.clustering.Clusterable;

import utils.DBSCAN.MyDoubleArrayDBS;
import ca.pfv.spmf.patterns.cluster.DoubleArray;

import com.goebl.simplify.Point;
import com.vividsolutions.jts.geom.Coordinate;



public class PointWrapper extends Coordinate implements Clusterable,Point{


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
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
		super.x = x;
		super.y = y;
		super.z = time;
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
		super.x = x;
	}

	public double getY() {
		return point[1];
	}

	public void setY(double y) {
		this.point[1] = y;
		super.y=y;
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
		return this.oid+":t="+this.time+":"+this.point[0]+","+this.point[1];
	}

	public MyDoubleArrayDBS getDoubleArray(){
		return new MyDoubleArrayDBS(new double[]{point[0],point[1],oid,time});
	}

}
