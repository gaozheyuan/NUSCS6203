package edu.nus.submodular.clustering.algorithm.impl;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

import edu.nus.submodular.clustering.algorithm.abst.AbstractAlgo;
import edu.nus.submodular.datainterface.DataInterface;
import edu.nus.submodular.macros.Macros;

public class KMeans extends AbstractAlgo implements DataInterface{
	public KMeans()
	{
		dataset=new ArrayList<double[]>();
	}
	public ArrayList<double[]> getCombinerRepresentationData(int numofData) {
		representations=new ArrayList<Integer>();
		ArrayList<double[]> result=new ArrayList<double[]>();
		for(int i=0;i<numofData;i++)
		{
			int pickIndex=pickNextCombinerElement();
			representations.add(pickIndex);
			result.add(dataset.get(pickIndex));
		}
		return result;
	}
	public ArrayList<double[]> getReducerRepresentationData(int numofData,ArrayList<double[]> candidatePoints) {
		representations=new ArrayList<Integer>();
		ArrayList<double[]> result=new ArrayList<double[]>();
		for(int i=0;i<numofData;i++)
		{
			int pickIndex=pickNextReducerElement(candidatePoints);
			representations.add(pickIndex);
			result.add(candidatePoints.get(pickIndex));
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

	public int pickNextCombinerElement() {
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
	public int pickNextReducerElement(ArrayList<double[]> candidatePoints)
	{
		double smallestError=Double.MAX_VALUE;
		int smallestIndex=-1;
		for(int index=0;index<candidatePoints.size();index++)
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
			result+=new Double(repData[index]).toString();
			result+=" ";
		}
		return result;
	}
	public void mapData(LongWritable ikey, Text ivalue, org.apache.hadoop.mapreduce.Mapper.Context context) {
		// TODO Auto-generated method stub
		Text texKey = new Text();
		texKey.set(Macros.MAPKEY);
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
	public void combineData(Text _key, Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer.Context  context){
		int numOfFeatures=0;
		Integer numOfElement=context.getConfiguration().getInt(Macros.NUMOFELEMENT, -1);
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
		ArrayList<double[]> repData=getCombinerRepresentationData(numOfElement);
		Text writeResult=new Text();
		for(int index=0;index<repData.size();index++)
		{
			writeResult=new Text();
			String output=convertRepDatatoString(repData.get(index));
			writeResult.set(output);
			try {
				System.err.println(output);
				context.write(_key, writeResult);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	public void reduceData(Text _key, Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer.Context context) {
		// TODO Auto-generated method stub
		Configuration conf=context.getConfiguration();
		Integer numOfElement=conf.getInt(Macros.NUMOFELEMENT, -1);
		String sourcePath=conf.get(Macros.INPUTPATH);
		int numOfFeatures=0;
		readSourceFile(sourcePath);
		ArrayList<double[]> candidateData=new ArrayList<double[]>();
		System.out.println("asd");
		int i=0;
		System.err.println(i);
		for (Text val : values) {
			System.out.println(val.toString());
			String[] strFeatures=val.toString().split(" ");
			numOfFeatures=strFeatures.length;
			double[] feature=new double[numOfFeatures];
			for(int indexFeature=0;indexFeature<numOfFeatures;indexFeature++)
			{
				feature[indexFeature]=Double.parseDouble(strFeatures[indexFeature]);
			}
			candidateData.add(feature);
		}
		ArrayList<double[]> repData=this.getReducerRepresentationData(numOfElement,candidateData);
		Text writeResult=new Text();
		for(int index=0;index<repData.size();index++)
		{
			writeResult=new Text();
			String output=convertRepDatatoString(repData.get(index));
			System.out.println(output);
			writeResult.set(output);
			try {
				context.write(_key, writeResult);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public void readSourceFile(String path)
	{
		Path dcPath=new Path(path);
		Set<String> result=new HashSet<String>();
		Configuration conf=new Configuration();
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(dcPath)));
			while (true) {
				String dataline=br.readLine();
				if(dataline==null)
					break;
				String originData=dataline.trim();
				String[] data = originData.split(" ");
				double[] doubledata=new double[data.length];
				dataset.add(doubledata);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
