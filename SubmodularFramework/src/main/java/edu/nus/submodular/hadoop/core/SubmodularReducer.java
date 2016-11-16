package edu.nus.submodular.hadoop.core;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.nus.submodular.clustering.algorithm.impl.KMeans;
import edu.nus.submodular.datainterface.DataInterface;
import edu.nus.submodular.graph.algorithm.impl.DistributedEdgeCover;
import edu.nus.submodular.graph.algorithm.impl.DistributedVertexCover;
import edu.nus.submodular.graph.algorithm.impl.GraphEdgeCover;
import edu.nus.submodular.graph.algorithm.impl.GraphVertexCover;
public class SubmodularReducer extends Reducer<Text, Text, Text, Text> {
	DataInterface inter=new KMeans();
	public SubmodularReducer()
	{
		System.out.println("Reducer created!");
	}
	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		inter.reduceData(_key, values, context);
	}
}
