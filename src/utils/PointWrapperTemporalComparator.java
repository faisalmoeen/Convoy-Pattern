package utils;

import java.util.Comparator;

import clustering.PointWrapper;

public class PointWrapperTemporalComparator implements Comparator<PointWrapper> {

	public PointWrapperTemporalComparator() {
	}

	@Override
	public int compare(PointWrapper o1, PointWrapper o2) {
		if(o1.getTime()>o2.getTime()){
			return 1;
		}else if(o1.getTime()==o2.getTime()){
			return 0;
		}
		else{
			return -1;
		}
	}

}
