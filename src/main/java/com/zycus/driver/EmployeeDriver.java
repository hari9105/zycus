package com.zycus.driver;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.zycus.mapper.EmployeeMapper;

public class EmployeeDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		
		if(args.length != 2){
			System.out.println("Two parameters Required");
			return -1;
		}
		
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();
		job.setJarByClass(EmployeeDriver.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(EmployeeMapper.class);
		
		job.setNumReduceTasks(0);
		DistributedCache.addCacheFile(new URI("/zycus/File1.txt"), conf); //loading one file in distributed cache for map side join
		boolean success = job.waitForCompletion(true);
		return success ? 0 :1;

	}
	
public static void main(String[] args) throws Exception{
		
		int exitCode = ToolRunner.run(new Configuration(),new EmployeeDriver(), args);
		System.exit(exitCode);
	}

}
