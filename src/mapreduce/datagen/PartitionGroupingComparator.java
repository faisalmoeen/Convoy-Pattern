package mapreduce.datagen;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PartitionGroupingComparator extends WritableComparator {

	public PartitionGroupingComparator() {
		super(PartitionTimePair.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		PartitionTimePair pair1 = (PartitionTimePair) a;
		PartitionTimePair pair2 = (PartitionTimePair) b;
		return pair1.getPartitionNo().compareTo(pair2.getPartitionNo());
	}

}
