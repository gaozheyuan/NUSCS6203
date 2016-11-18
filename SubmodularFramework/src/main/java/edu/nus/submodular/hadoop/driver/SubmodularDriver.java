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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.nus.submodular.hadoop.core.SubmodularCombiner;
import edu.nus.submodular.hadoop.core.SubmodularMapper;
import edu.nus.submodular.hadoop.core.SubmodularReducer;
import edu.nus.submodular.macros.Macros;
public class SubmodularDriver {
	
	public static void main(String[] args) throws Exception {
	
		Configuration conf = new Configuration();
		conf.set(Macros.PROGRAMNAME, args[0]);
		conf.set(Macros.INPUTPATH, args[1]);
		Integer numOfElement=Integer.parseInt(args[3]);
		conf.setInt(Macros.NUMOFELEMENT, numOfElement);
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(edu.nus.submodular.hadoop.driver.SubmodularDriver.class);
		job.setMapperClass(SubmodularMapper.class);
		job.setCombinerClass(SubmodularCombiner.class);
		job.setReducerClass(SubmodularReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		if (!job.waitForCompletion(true))
			return;
	}
}
