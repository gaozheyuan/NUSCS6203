package edu.nus.submodular.hadoop.core;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.nus.submodular.clustering.algorithm.impl.KMeans;

public class SubmodularCombiner extends Reducer<Text, Text, Text, Text> {
	KMeans inter=new KMeans();
	
	public SubmodularCombiner()
	{
		System.out.println("Combiner Created!");
	}
	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int numOfFeatures=0;
		for (Text val : values) {
			String[] strFeatures=val.toString().split(" ");
			numOfFeatures=strFeatures.length;
			double[] feature=new double[numOfFeatures];
			for(int indexFeature=0;indexFeature<numOfFeatures;indexFeature++)
			{
				feature[indexFeature]=Double.parseDouble(strFeatures[indexFeature]);
			}
			inter.getDataset().add(feature);
		}
		ArrayList<double[]> repData=inter.getRepresentationData(5);
		Text writeResult=new Text();
		for(int index=0;index<repData.size();index++)
		{
			writeResult=new Text();
			String output=convertRepDatatoString(repData.get(index));
			writeResult.set(output);
			context.write(_key, writeResult);
		}
	}
	private String convertRepDatatoString(double[] repData)
	{
		String result="";
		for(int index=0;index<repData.length;index++)
		{
			result.concat(new Double(repData[index]).toString());
			result.concat(" ");
		}
		return result;
	}
}
