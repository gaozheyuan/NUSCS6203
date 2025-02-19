package edu.nus.submodular.clustering.algorithm.impl;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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
		representations=new ArrayList<double[]>();
		for(int i=0;i<numofData;i++)
		{
			double[] nextElement=pickNextCombinerElement();//every time pick up the best one
			representations.add(nextElement);
		}
		return representations;
	}
	public ArrayList<double[]> getReducerRepresentationData(int numofData,ArrayList<double[]> candidatePoints) {
		representations=new ArrayList<double[]>();
		System.out.println("num of data"+numofData);
		for(int i=0;i<numofData;i++)
		{
			System.out.println(i);
			double[] nextElement=pickNextReducerElement(candidatePoints);
			representations.add(nextElement);
		}
		return representations;
	}
	public double calculateTotalError(ArrayList<double[]> rep) {
		double totalError=0;
		int[] labels=assignLabel(rep);//assign every node to a group
		for(int index=0;index<dataset.size();index++)
		{
			totalError+=calculateTwoElementsError(dataset.get(index),rep.get(labels[index]));
		}
		return totalError;
	}

	public int[] assignLabel(ArrayList<double[]> rep) {
		int[] result=new int[dataset.size()];
		for(int index=0;index<dataset.size();index++)
		{
			int smallestIndex=-1;
			double smallesterror=Double.MAX_VALUE;
			//We assign the index to each element in the dataset as the label
			for(int repIndex=0;repIndex<rep.size();repIndex++)
			{
				double error=calculateTwoElementsError(dataset.get(index),rep.get(repIndex));
				if(smallesterror>error)
				{
					smallesterror=error;
					smallestIndex=repIndex;
				}
			}
			result[index]=smallestIndex;
		}
		return result;
	}
	public boolean containElement(ArrayList<double[]> sets,double[] element)
	{
		for(int i=0;i<sets.size();i++)
		{
			boolean singleResult=true;
			for(int j=0;j<element.length;j++)
			{
				if(sets.get(i)[j]!=element[j])
				{
					singleResult=false;
					break;
				}
			}
			if(singleResult==true)
				return true;
		}
		return false;
	}
	public double[] pickNextCombinerElement() {
		double smallestError=Double.MAX_VALUE;
		int smallestIndex=-1;
		for(int index=0;index<dataset.size();index++)
		{
			ArrayList<double[]> newRepresentations=(ArrayList<double[]>)representations.clone();
			if(!containElement(representations, dataset.get(index)))
			{
				newRepresentations.add(dataset.get(index));
				double totalerror=calculateTotalError(newRepresentations);
				if(smallestError>totalerror)
				{
					smallestError=totalerror;
					smallestIndex=index;
				}
			}
		}
		return dataset.get(smallestIndex);
	}
	public double[] pickNextReducerElement(ArrayList<double[]> candidatePoints)
	{
		double smallestError=Double.MAX_VALUE;
		int smallestIndex=-1;
		for(int index=0;index<candidatePoints.size();index++)
		{
			System.err.print("candidate index"+index);
			ArrayList<double[]> newRepresentations=(ArrayList<double[]>)representations.clone();
			if(!containElement(representations, candidatePoints.get(index)))
			{
				newRepresentations.add(candidatePoints.get(index));
				double totalerror=calculateTotalError(newRepresentations);
				System.out.println("totalerror "+totalerror);
				if(smallestError>totalerror)
				{
					smallestError=totalerror;
					smallestIndex=index;
				}
			}
		}
		System.err.println("hahaha");
		System.err.println("smallest index"+ smallestIndex);
		return candidatePoints.get(smallestIndex);
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
			getDataset().add(feature);
		}
		System.out.println("asdasd");
		ArrayList<double[]> repData=getCombinerRepresentationData(numOfElement);
		System.out.println(repData.size());
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
		for (Text val : values) {
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
			writeResult.set(output);
			Text resultkey=new Text();
			resultkey.set("result");
			try {
				context.write(resultkey, writeResult);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//output the error value
		double totalerror=this.calculateTotalError(repData);
		Text errorkey=new Text();
		errorkey.set("totalerror");
		writeResult=new Text();
		writeResult.set(new Double(totalerror).toString() );
		try {
			context.write(errorkey, writeResult);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//output the number of elements in each group
		int[] label=this.assignLabel(repData);
		Set<Integer> labelSet=new HashSet<Integer>();
		for(int element:label)
		{
			labelSet.add(element);
		}
		Integer[] num=new Integer[labelSet.size()];
		for(int i=0;i<num.length;i++)
			num[i]=0;
		for(Integer element:label)
			num[element]++;
		Text numkey=new Text();
		numkey.set("groupSize");
		for(int i=0;i<num.length;i++)
		{
			writeResult=new Text();
			writeResult.set(new Integer(num[i]).toString());
			try {
				context.write(numkey, writeResult);
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
			if(!fs.isDirectory(dcPath))
			{
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(dcPath)));
				while (true) {
					String dataline=br.readLine();
					if(dataline==null)
						break;
					String originData=dataline.trim();
					System.out.println("read data"+originData);
					String[] strdata = originData.split(" ");
					double[] doubledata=new double[strdata.length];
					for(int i=0;i<strdata.length;i++)
						doubledata[i]=Double.parseDouble(strdata[i]);
					dataset.add(doubledata);
				}
			}
			else
			{
				FileStatus[] status=fs.listStatus(dcPath);
				for(int index=0;index<status.length;index++)
				{
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[index].getPath())));
					while (true) {
						String dataline=br.readLine();
						if(dataline==null)
							break;
						String originData=dataline.trim();
						String[] strdata = originData.split(" ");
						double[] doubledata=new double[strdata.length];
						for(int i=0;i<strdata.length;i++)
							doubledata[i]=Double.parseDouble(strdata[i]);
						dataset.add(doubledata);
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
