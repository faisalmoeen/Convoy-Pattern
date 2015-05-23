package mapreduce.datagen;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TimePartitioner extends Partitioner<PartitionTimePair, Text> {

	@Override
	public int getPartition(PartitionTimePair pair, Text text, int numPartitions) {
		
		return pair.getPartitionNo().get();
	}

}
