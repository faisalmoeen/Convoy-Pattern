package clustering;

import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.Clusterable;


public class ClusterVcoda<T extends Clusterable> extends Cluster<T> {
	
//	private static final long serialVersionUID = -3442297081515880461L;

	private int startTime;
	private int endTime;
	private boolean matched=false;
	private boolean absorbed=false;
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
	public boolean isMatched() {
		return matched;
	}
	public void setMatched(boolean matched) {
		this.matched = matched;
	}
	public boolean isAbsorbed() {
		return absorbed;
	}
	public void setAbsorbed(boolean absorbed) {
		this.absorbed = absorbed;
	}
	
	
}
