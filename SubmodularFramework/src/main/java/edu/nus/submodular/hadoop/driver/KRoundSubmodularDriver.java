package edu.nus.submodular.hadoop.driver;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.nus.submodular.hadoop.core.SubmodularCombiner;
import edu.nus.submodular.hadoop.core.SubmodularMapper;
import edu.nus.submodular.hadoop.core.SubmodularReducer;
import edu.nus.submodular.macros.Macros;


public class KRoundSubmodularDriver {

	public static void main(String[] args) throws Exception {
		int numOfElements=1;
		while(true)
		{
			Configuration conf = new Configuration();
			conf.setInt(Macros.STRINGELEMENT,numOfElements);
			Job job = Job.getInstance(conf, "JobName");
			job.setJarByClass(edu.nus.submodular.hadoop.driver.SubmodularDriver.class);
			job.setMapperClass(SubmodularMapper.class);
			job.setCombinerClass(SubmodularCombiner.class);
			job.setReducerClass(SubmodularReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			if (!job.waitForCompletion(true))
				return;
		/*	Path pt=new Path(Macros.RECORDFILE);
			FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line=br.readLine();*/
		}
	}
}
