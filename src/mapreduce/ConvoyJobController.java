package mapreduce;
import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ConvoyJobController {


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
    conf.set("mapreduce.map.output.compress", "false");
    Job job = Job.getInstance(conf, "Distributed Convoy");
    job.setJarByClass(ConvoyJobController.class);
    job.setMapperClass(ConvoyMapper.class);
//    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(ConvoyReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    for(String s:f.list()){
    	MultipleInputs.addInputPath(job,new Path(args[0]+"/"+s),TextInputFormat.class,ConvoyMapper.class);
    }
//    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
