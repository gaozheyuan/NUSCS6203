package edu.nus.submodular.hadoop.core;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.nus.submodular.clustering.algorithm.impl.KMeans;
import edu.nus.submodular.datainterface.DataInterface;
import edu.nus.submodular.graph.algorithm.impl.DistributedEdgeCover;
import edu.nus.submodular.graph.algorithm.impl.DistributedVertexCover;
import edu.nus.submodular.graph.algorithm.impl.GraphEdgeCover;
import edu.nus.submodular.graph.algorithm.impl.GraphVertexCover;
import edu.nus.submodular.macros.Macros;

public class SubmodularMapper extends Mapper<LongWritable, Text, Text, Text> {
	DataInterface inter;
	public SubmodularMapper()
	{
		System.out.print("mapper created!");
	}
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		String programname=context.getConfiguration().get(Macros.PROGRAMNAME);
		switch(programname)
		{
		case "KMeans":
			try {
				inter=(KMeans)Class.forName("edu.nus.submodular.clustering.algorithm.impl.KMeans").newInstance();
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		inter.mapData(ikey, ivalue, context);
	}
}
