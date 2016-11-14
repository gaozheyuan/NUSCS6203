package edu.nus.submodular.hadoop.driver;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.nus.submodular.hadoop.core.SubmodularClusterCombiner;
import edu.nus.submodular.hadoop.core.SubmodularClusterMapper;
import edu.nus.submodular.hadoop.core.SubmodularClusterReducer;

public class SubmodularDriver {
	
	public static void main(String[] args) throws Exception {
	
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(edu.nus.submodular.hadoop.driver.SubmodularDriver.class);
		job.setMapperClass(SubmodularClusterMapper.class);
		job.setCombinerClass(SubmodularClusterCombiner.class);
		job.setReducerClass(SubmodularClusterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		if (!job.waitForCompletion(true))
			return;
	}
}
