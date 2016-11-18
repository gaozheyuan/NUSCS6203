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
import edu.nus.submodular.macros.Macros;

public class SubmodularCombiner extends Reducer<Text, Text, Text, Text> {
	DataInterface inter;
	public SubmodularCombiner()
	{
		System.out.println("Combiner Created!");
	
	}
	public void reduce(Text _key, Iterable<Text> values, Context context)
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
			break;
		case "GraphEdgeCover":
			try {
				inter=(KMeans)Class.forName("edu.nus.submodular.graph.algorithm.impl.GraphEdgeCover").newInstance();
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		case "GraphVertexCover":
			try {
				inter=(KMeans)Class.forName("edu.nus.submodular.graph.algorithm.impl.GraphVertexCover").newInstance();
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		}
		inter.combineData(_key, values, context);
	}
}
