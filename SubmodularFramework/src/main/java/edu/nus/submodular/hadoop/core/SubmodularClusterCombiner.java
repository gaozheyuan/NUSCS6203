package edu.nus.submodular.hadoop.core;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.nus.submodular.clustering.algorithm.impl.KMeans;
import edu.nus.submodular.datainterface.DataInterface;

public class SubmodularClusterCombiner extends Reducer<Text, Text, Text, Text> {
	DataInterface inter=new KMeans();
	public SubmodularClusterCombiner()
	{
		System.out.println("Combiner Created!");
	}
	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		inter.combineData(_key, values, context);
	}
}
