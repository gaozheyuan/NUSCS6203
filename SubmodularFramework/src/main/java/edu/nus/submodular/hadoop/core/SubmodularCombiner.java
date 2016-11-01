package edu.nus.submodular.hadoop.core;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.nus.submodular.algorithm.abs.InterfaceAlgo;
import edu.nus.submodular.algorithm.impl.KMeans;

public class SubmodularCombiner extends Reducer<Text, Text, Text, Text> {
	KMeans inter=new KMeans();
	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		
		int numOfInstances=0;
		int numOfFeatures=0;
		for (Text val : values) {
			numOfInstances++;
			numOfFeatures=val.toString().split(" ").length;
		}
		double[][] data=new double[numOfInstances][numOfFeatures];
		int index=0;
		for (Text val : values) {
			String[] strFeatures=val.toString().split(" ");
			for(int indexFeature=0;indexFeature<strFeatures.length;indexFeature++)
			{
				data[index][indexFeature]=Double.parseDouble(strFeatures[indexFeature]);
			}
			index++;
		}
		inter.setDataset(data);
		ArrayList<double[]> repData=inter.getRepresentationData(5);
		Text writeResult=new Text();
		for(int index1=0;index1<repData.size();index1++)
		{
			writeResult=new Text();
			String output=convertRepDatatoString(repData.get(index1));
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
