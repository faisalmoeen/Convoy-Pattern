package mapreduce;
import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ConvoyJobWithMRSplits {

	public static void main(String[] args) throws Exception {
		File f= new File(args[0]);
		File out = new File(args[1]);
		if(out.exists()){
			FileUtils.deleteDirectory(out);
		}
		Configuration conf = new Configuration();
		//    conf.set("m", "3");
		//    conf.set("k", "180");
		//    conf.set("e", "0.0006");
		conf.set("m", args[2]);
		conf.set("k", args[3]);
		conf.set("e", args[4]);
		conf.set("gMinTime", args[5]);
		conf.set("gMaxTime", args[6]);
		conf.set("mapreduce.map.output.compress", "false");
//		conf.set("mapred.max.split.size", "3000000000");
//		conf.set("mapred.max.split.size", "300000");
//		conf.set("mapred.min.split.size", "3000000000");
//		conf.set("mapred.min.split.size", "300000");
		conf.set("output.dir", args[1]);
		Job job = Job.getInstance(conf, "Distributed Convoy");
		job.setJarByClass(ConvoyJobWithMRSplits.class);
		job.setMapperClass(ConvoyMapper.class);
		//    job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(ConvoyReducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(CustomTemporalFileInputFormat.class);

//		Counters c = job.getCounters(); 
//		c.findCounter(ConvoyCounters.JOB_START).setValue(System.currentTimeMillis());

		CustomTemporalFileInputFormat.setInputDirRecursive(job, true);
		CustomTemporalFileInputFormat.addInputPath(job, new Path(args[0]));
		//    MultipleInputs.addInputPath(job,new Path(args[0]+"/"+s),TextInputFormat.class,ConvoyMapper.class);
		//    for(String s:f.list()){
		//    	MultipleInputs.addInputPath(job,new Path(args[0]+"/"+s),TextInputFormat.class,ConvoyMapper.class);
		//    }
		//    FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
