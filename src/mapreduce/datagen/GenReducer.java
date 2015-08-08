package mapreduce.datagen;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GenReducer extends Reducer<PartitionTimePair,Text,Text,NullWritable> {

	private IntWritable one = new IntWritable(1);
	

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		try {
			Configuration conf = context.getConfiguration();
			super.setup(context);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void reduce(PartitionTimePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		try {
			for (Text val : values) {
//				System.out.println(val);
				context.write(val, null);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}