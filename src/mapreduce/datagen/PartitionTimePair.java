package mapreduce.datagen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class PartitionTimePair implements WritableComparable<PartitionTimePair> {

	private IntWritable partitionNo;
	public IntWritable getPartitionNo() {
		return partitionNo;
	}
	public void setPartitionNo(IntWritable partitionNo) {
		this.partitionNo = partitionNo;
	}
	public IntWritable getTime() {
		return time;
	}
	public void setTime(IntWritable time) {
		this.time = time;
	}

	private IntWritable time;
	
	public PartitionTimePair() {
		super();
		this.partitionNo = new IntWritable();
		this.time = new IntWritable();
	}
	public PartitionTimePair(int partition,int time) {
		this.partitionNo = new IntWritable(partition);
		this.time = new IntWritable(time);
	}
	@Override
	public void readFields(DataInput input) throws IOException {
		try {
			partitionNo.readFields(input);
			time.readFields(input);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(DataOutput output) throws IOException {
		partitionNo.write(output);
		time.write(output);
	}

	@Override
	public int compareTo(PartitionTimePair o) {
		int cmp = partitionNo.compareTo(o.partitionNo);
		if(cmp != 0){
			return cmp;
		}
		return time.compareTo(o.time);
	}

}
