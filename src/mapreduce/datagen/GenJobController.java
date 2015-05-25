package mapreduce.datagen;
import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GenJobController {


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
    conf.set("nr", args[2]);//No. of replications
    conf.set("np", args[3]);//No. of partitions
    conf.set("ts", args[4]);//Time shift per replication
    conf.set("xs", args[5]);//X shift per replication
    conf.set("ys", args[6]);//Y shift per replication
    Job job = Job.getInstance(conf, "Distributed Convoy");
    job.setJarByClass(GenJobController.class);
    job.setMapperClass(GenMapper.class);
//    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(GenReducer.class);
    job.setNumReduceTasks(Integer.parseInt(args[3]));
    job.setMapOutputKeyClass(PartitionTimePair.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    
    job.setPartitionerClass(TimePartitioner.class);
    job.setGroupingComparatorClass(PartitionGroupingComparator.class);
    TextInputFormat.addInputPath(job, new Path(args[0]));
    TextOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
