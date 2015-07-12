package utils.DBSCAN;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.math3.geometry.euclidean.twod.Line;
import org.apache.commons.math3.geometry.euclidean.twod.Vector2D;
import org.apache.commons.math3.ml.clustering.Clusterable;

import utils.TemporalLine;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;

import clustering.PointWrapper;

public class Trajectory implements Clusterable{

	private LinkedList<PointWrapper> pointList;
	private int oid;
	private long start;
	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	private long end;
	public int getOid() {
		return oid;
	}

	public void setOid(int oid) {
		this.oid = oid;
	}

	public LinkedList<PointWrapper> getPointList() {
		return pointList;
	}

	public void setPointList(LinkedList<PointWrapper> pointList) {
		this.pointList = pointList;
	}

	public Trajectory() {
	}

	public Trajectory(int oid, LinkedList<PointWrapper> pointList) {
		this.pointList = pointList;
		this.oid = oid;
		start = pointList.getFirst().getTime();
		end = pointList.getLast().getTime();
	}
	
	@Override
	public double[] getPoint() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public double distance(Trajectory b){
		//construct a line from array of points and calculate the distance
//		Coordinate[] coordinates1 = (Coordinate[]) pointList.toArray(new PointWrapper[(pointList.size())]);
//		Coordinate[] coordinates2 = b.getPointList().toArray(new PointWrapper[(b.getPointList().size())]);
		//***************************
		List<LineString> lineSegments1 = createLineSegments(this);
		List<LineString> lineSegments2 = createLineSegments(b);
		Iterator<LineString> lines1 = lineSegments1.iterator();
		Iterator<LineString> lines2 = lineSegments2.iterator();
		LineString line1 = (lines1.hasNext()?lines1.next():null);
		LineString line2 = (lines2.hasNext()?lines2.next():null);
		double minDistance = 99999;
		long ta1 = ((PointWrapper)line1.getCoordinateN(0)).getTime();
		long ta2 = ((PointWrapper)line1.getCoordinateN(1)).getTime();
		long tb1 = ((PointWrapper)line1.getCoordinateN(0)).getTime();
		long tb2 = ((PointWrapper)line1.getCoordinateN(1)).getTime();
		double distance=0;
		for(long t=b.start;t<=b.end;t++){
			if(!between(t,ta1,ta2)){
				line1 = (lines1.hasNext()?lines1.next():null);
			}
			if(!between(t,tb1,tb2)){
				line2 = (lines2.hasNext()?lines2.next():null);
			}
			if(line1==null || line2==null){
				break;
			}
			distance = line1.distance(line2);
			if(distance<minDistance){
				minDistance=distance;
			}
		}
//		Geometry g1 = new GeometryFactory().createLineString(coordinates1);
//		Geometry g2 = new GeometryFactory().createLineString(coordinates2);
		return minDistance;
	}
	
//	public double distance(Trajectory b){
//		//construct a line from array of points and calculate the distance
////		Coordinate[] coordinates1 = (Coordinate[]) pointList.toArray(new PointWrapper[(pointList.size())]);
////		Coordinate[] coordinates2 = b.getPointList().toArray(new PointWrapper[(b.getPointList().size())]);
//		//***************************
//		List<TemporalLine> lineSegments1 = createLineApacheSegments(this);
//		List<TemporalLine> lineSegments2 = createLineApacheSegments(b);
//		Iterator<TemporalLine> lines1 = lineSegments1.iterator();
//		Iterator<TemporalLine> lines2 = lineSegments2.iterator();
//		TemporalLine line1 = (lines1.hasNext()?lines1.next():null);
//		TemporalLine line2 = (lines2.hasNext()?lines2.next():null);
//		double minDistance = 99999;
//		long ta1 = line1.getStartTime();
//		long ta2 = line1.getEndTime();
//		long tb1 = line2.getStartTime();
//		long tb2 = line2.getEndTime();
//		double distance=0;
//		for(long t=b.start;t<=b.end;t++){
//			if(!between(t,ta1,ta2)){
//				line1 = (lines1.hasNext()?lines1.next():null);
//			}
//			if(!between(t,tb1,tb2)){
//				line2 = (lines2.hasNext()?lines2.next():null);
//			}
//			if(line1==null || line2==null){
//				break;
//			}
//			distance = line1.distance(line2);
//			if(distance<minDistance){
//				minDistance=distance;
//			}
//		}
////		Geometry g1 = new GeometryFactory().createLineString(coordinates1);
////		Geometry g2 = new GeometryFactory().createLineString(coordinates2);
//		return minDistance;
//	}
	private boolean between(long t,long ta,long tb){
		if(t>=ta && t<=tb){
			return true;
		}
		return false;
	}
	private List<LineString> createLineSegments(Trajectory x){
		List<LineString> ls = new ArrayList<LineString>();
		GeometryFactory gf = new GeometryFactory();
		Iterator<PointWrapper> i = x.getPointList().iterator();
		Coordinate a=null;
		if(i.hasNext()){
			a= i.next();
		}
		while(i.hasNext()){
			Coordinate b = i.next();
			ls.add(gf.createLineString(new Coordinate[]{a,b}));
			a=b;
		}
		return ls;
	}
	private List<TemporalLine> createLineApacheSegments(Trajectory x){
		List<TemporalLine> ls = new ArrayList<TemporalLine>();
		
		Iterator<PointWrapper> i = x.getPointList().iterator();
		PointWrapper a=null;
		if(i.hasNext()){
			a= i.next();
		}
		while(i.hasNext()){
			PointWrapper b = i.next();
			ls.add(new TemporalLine(a, b,a.getTime(),b.getTime()));
			a=b;
		}
		return ls;
	}

}
