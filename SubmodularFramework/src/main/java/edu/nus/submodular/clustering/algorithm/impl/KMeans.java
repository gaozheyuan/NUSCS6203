package edu.nus.submodular.clustering.algorithm.impl;
import java.util.ArrayList;
import edu.nus.submodular.algorithm.inter.InterfaceAlgo;
import edu.nus.submodular.clustering.algorithm.abst.AbstractAlgo;

public class KMeans extends AbstractAlgo implements InterfaceAlgo{
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
}
