package edu.nus.submodular.hadoop.core;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import edu.nus.submodular.clustering.algorithm.impl.KMeans;
public class SubmodularClusterReducer extends Reducer<Text, Text, Text, Text> {
	KMeans inter=new KMeans();
	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		inter.reduceData(_key, values, context);
	}
}
