package edu.nus.submodular.hadoop.core;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SubmodularClusterMapper extends Mapper<LongWritable, Text, Text, Text> {
	DataInterface inter=new KMeans();
	public SubmodularClusterMapper()
	{
		System.out.print("mapper created!");
	}
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		inter.mapData(ikey, ivalue, context);
	}
}
