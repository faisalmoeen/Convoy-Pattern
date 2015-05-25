package mapreduce.datagen;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.Utils;
import clustering.DbscanFile;
import clustering.PointWrapper;
import base.Convoy;
import base.Vcoda;

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
				System.out.println(val);
				context.write(val, null);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}