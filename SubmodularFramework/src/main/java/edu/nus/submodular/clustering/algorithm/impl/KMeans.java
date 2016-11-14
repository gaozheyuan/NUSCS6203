package edu.nus.submodular.clustering.algorithm.impl;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

import edu.nus.submodular.clustering.algorithm.abst.AbstractAlgo;
import edu.nus.submodular.datainterface.DataInterface;

public class KMeans extends AbstractAlgo implements DataInterface{
	public KMeans()
	{
		dataset=new ArrayList<double[]>();
	}
	public ArrayList<double[]> getRepresentationData(int numofData) {
		representations=new ArrayList<Integer>();
		ArrayList<double[]> result=new ArrayList<double[]>();
		for(int i=0;i<numofData;i++)
		{
			int pickIndex=pickNextElement();
			representations.add(pickIndex);
			result.add(dataset.get(pickIndex));
		}
		return result;
	}
	
	public double calculateTotalError(ArrayList<Integer> representations) {
		double totalError=0;
		int[] labels=assignLabel(representations);
		for(int index=0;index<this.dataset.size();index++)
		{
			totalError+=calculateTwoElementsError(dataset.get(index),dataset.get(labels[index]));
		}
		System.out.println("Total error "+totalError);
		return totalError;
	}
	
	public int[] assignLabel(ArrayList<Integer> representations) {
		int[] result=new int[dataset.size()];
		for(int index=0;index<dataset.size();index++)
		{
			int smallestIndex=0;
			double smallesterror=Double.MAX_VALUE;
			//We assign the index to each element in the dataset as the label
			for(int repIndex=0;repIndex<representations.size();repIndex++)
			{
				double error=calculateTwoElementsError(dataset.get(index),dataset.get(representations.get(repIndex)));
				if(smallesterror>error)
				{
					smallesterror=error;
					smallestIndex=representations.get(repIndex);
				}
			}
			result[index]=smallestIndex;
		}
		return result;
	}

	public int pickNextElement() {
		double smallestError=Double.MAX_VALUE;
		int smallestIndex=-1;
		for(int index=0;index<dataset.size();index++)
		{
			ArrayList<Integer> newRepresentations=(ArrayList<Integer>)representations.clone();
			if(!representations.contains(index))
			{
				newRepresentations.add(index);
				double totalerror=calculateTotalError(newRepresentations);
				if(smallestError>totalerror)
				{
					smallestError=totalerror;
					smallestIndex=index;
				}
			}
		}
		return smallestIndex;
	}

	public double calculateTwoElementsError(double[] first, double[] second) {
		double result=0;
		double[] errorArray=new double[first.length];
		for(int i=0;i<first.length;i++)
		{
			errorArray[i]=first[i]-second[i];
			result+=errorArray[i]*errorArray[i];
		}
		result=Math.sqrt(result);
		return result;
	}

	public void loadData(String fileName) {
		
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
	public void mapData(LongWritable ikey, Text ivalue, org.apache.hadoop.mapreduce.Mapper.Context context) {
		// TODO Auto-generated method stub
		Text texKey = new Text();
		texKey.set("1");
		System.out.println(ivalue.toString());
		try {
			context.write(texKey, ivalue);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void combineData(Text _key, Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer.Context  context) throws IOException, InterruptedException {
		int numOfFeatures=0;
		for (Text val : values) {
			String[] strFeatures=val.toString().split(" ");
			numOfFeatures=strFeatures.length;
			double[] feature=new double[numOfFeatures];
			for(int indexFeature=0;indexFeature<numOfFeatures;indexFeature++)
			{
				feature[indexFeature]=Double.parseDouble(strFeatures[indexFeature]);
			}
			this.getDataset().add(feature);
		}
		ArrayList<double[]> repData=getRepresentationData(5);
		Text writeResult=new Text();
		for(int index=0;index<repData.size();index++)
		{
			writeResult=new Text();
			String output=convertRepDatatoString(repData.get(index));
			writeResult.set(output);
			context.write(_key, writeResult);
		}
		
	}
	public void reduceData(Text _key, Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int numOfFeatures=0;
		for (Text val : values) {
			String[] strFeatures=val.toString().split(" ");
			numOfFeatures=strFeatures.length;
			double[] feature=new double[numOfFeatures];
			for(int indexFeature=0;indexFeature<numOfFeatures;indexFeature++)
			{
				feature[indexFeature]=Double.parseDouble(strFeatures[indexFeature]);
			}
			getDataset().add(feature);
		}
		ArrayList<double[]> repData=getRepresentationData(5);
		Text writeResult=new Text();
		for(int index=0;index<repData.size();index++)
		{
			writeResult=new Text();
			String output=convertRepDatatoString(repData.get(index));
			writeResult.set(output);
			context.write(_key, writeResult);
		}
	}
}
