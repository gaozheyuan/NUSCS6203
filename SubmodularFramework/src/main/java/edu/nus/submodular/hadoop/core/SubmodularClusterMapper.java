package edu.nus.submodular.hadoop.core;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.nus.submodular.clustering.algorithm.impl.KMeans;
import edu.nus.submodular.datainterface.DataInterface;
import edu.nus.submodular.graph.algorithm.impl.GraphEdgeCover;

public class SubmodularClusterMapper extends Mapper<LongWritable, Text, Text, Text> {
	DataInterface inter=new GraphEdgeCover();
	public SubmodularClusterMapper()
	{
		System.out.print("mapper created!");
	}
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		inter.mapData(ikey, ivalue, context);
	}
}
