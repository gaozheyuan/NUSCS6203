package edu.nus.submodular.hadoop.driver;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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
		double targetResult=Double.MAX_VALUE,roundResult=0;
		int round=0;
		while(roundResult<targetResult)
		{
			if(round==1)
				numOfElements=1;
			Configuration conf = new Configuration();
			conf.setInt(Macros.NUMOFELEMENT,numOfElements);
			conf.setDouble(Macros.ALPHA, 0.5);
			conf.setDouble(Macros.LAMDA, 0.1);
			conf.setDouble(Macros.CONSTRAINT, targetResult);
			Job job = Job.getInstance(conf, "JobName");
			job.setJarByClass(edu.nus.submodular.hadoop.driver.SubmodularDriver.class);
			job.setMapperClass(SubmodularMapper.class);
			job.setCombinerClass(SubmodularCombiner.class);
			job.setReducerClass(SubmodularReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			FileSystem  hdfs = FileSystem.get(conf);
			if(hdfs.exists(new Path(args[1])))
				hdfs.delete(new Path(args[1]),true);
			if (!job.waitForCompletion(true))
				return;
			FileSystem fs = FileSystem.get(conf);
			Path pt=new Path(Macros.TARGETRESULTFILE);
            BufferedReader br1=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String strTargetResult=br1.readLine();
            targetResult=Double.parseDouble(strTargetResult);          
            pt=new Path(Macros.ROUNDRESULTFILE);
            br1=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String strRoundResult=br1.readLine();
            roundResult=Double.parseDouble(strRoundResult);
            round++;
            numOfElements++;
            System.out.println(round);
		}
	}
	public static void combineTwoFiles(String dcFile,String gdFile, Configuration conf)
	{
		Path dcPath=new Path(dcFile);
		Path gdPath=new Path(gdFile);
		Set<String> result=new HashSet<String>();
		try {
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(dcPath)));
			while(true)
			{
				String node=br.readLine();
				if(node==null)
					break;
				result.add(node.trim());
			}
			br=new BufferedReader(new InputStreamReader(fs.open(gdPath)));
			while(true)
			{
				String node=br.readLine();
				if(node==null)
					break;
				result.add(node.trim());
			}
			FSDataOutputStream out=fs.create(dcPath);
			Iterator<String> resultIter=result.iterator();
			while(resultIter.hasNext())
			{
				out.writeBytes(resultIter.next());
				out.writeChar('\n');
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
