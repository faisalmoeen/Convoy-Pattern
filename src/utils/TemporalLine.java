package utils;

import org.apache.commons.math3.geometry.euclidean.twod.Line;
import org.apache.commons.math3.geometry.euclidean.twod.Vector2D;

import clustering.PointWrapper;

import com.vividsolutions.jts.geom.Coordinate;

public class TemporalLine extends Line{

	private long startTime;
	private long endTime;
	public TemporalLine(PointWrapper p1, PointWrapper p2, long t1, long t2) {
		super(new Vector2D(p1.getPoint()), new Vector2D(p2.getPoint()));
		this.startTime = t1;
		this.endTime = t2;
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
}
