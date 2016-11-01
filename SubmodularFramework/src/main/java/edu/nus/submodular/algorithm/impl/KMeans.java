package edu.nus.submodular.algorithm.impl;
import java.util.ArrayList;
import edu.nus.submodular.algorithm.abs.AbstractAlgo;
import edu.nus.submodular.algorithm.abs.InterfaceAlgo;

public class KMeans extends AbstractAlgo implements InterfaceAlgo{

	public ArrayList<double[]> getRepresentationData(int numofData) {
		representations=new ArrayList<Integer>();
		ArrayList<double[]> result=new ArrayList<double[]>();
		for(int i=0;i<numofData;i++)
		{
			int pickIndex=pickNextElement();
			representations.add(pickIndex);
			result.add(dataset[pickIndex]);
		}
		return result;
	}
	
	public double calculateTotalError(ArrayList<Integer> representations) {
		double totalError=0;
		int[] labels=assignLabel(representations);
		for(int index=0;index<this.dataset.length;index++)
		{
			totalError+=calculateTwoElementsError(dataset[index],dataset[labels[index]]);
		}
		System.out.println("Total error "+totalError);
		return totalError;
	}
	
	public int[] assignLabel(ArrayList<Integer> representations) {
		int[] result=new int[dataset.length];
		for(int index=0;index<dataset.length;index++)
		{
			int smallestIndex=0;
			double smallesterror=Double.MAX_VALUE;
			//We assign the index to each element in the dataset as the label
			for(int repIndex=0;repIndex<representations.size();repIndex++)
			{
				double error=calculateTwoElementsError(this.dataset[index],dataset[representations.get(repIndex)]);
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
		for(int index=0;index<dataset.length;index++)
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
	
	public static void main(String[] args)
	{
		double marks[][]=
			{
					{0.0  ,8.0 ,50.0 ,31.0 ,12.0, 48.0 ,36.0 , 2.0,  5.0 ,39.0 ,10.0},
					{8.0 , 0.0 ,38.0 , 9.0 ,33.0 ,37.0 ,22.0 , 6.0  ,4.0 ,14.0 ,32.0},
					{50.0, 38.0 , 0.0 ,11.0, 55.0,  1.0 ,23.0 ,46.0 ,41.0 ,17.0 ,52.0},
					{31.0,  9.0, 11.0,  0.0, 44.0 ,13.0, 16.0 ,19.0 ,25.0, 18.0, 42.0},
					{12.0, 33.0, 55.0, 44.0,  0.0, 54.0, 53.0 ,30.0 ,28.0 ,45.0 , 7.0},
					{48.0, 37.0,  1.0, 13.0, 54.0,  0.0, 26.0, 47.0, 40.0, 24.0, 51.0},
					{36.0, 22.0, 23.0, 16.0, 53.0, 26.0,  0.0, 29.0, 35.0, 34.0, 49.0},
					{2.0 , 6.0 ,46.0, 19.0 ,30.0, 47.0 ,29.0 , 0.0 , 3.0 ,27.0, 15.0},
					{5.0 , 4.0 ,41.0, 25.0, 28.0, 40.0, 35.0,  3.0,  0.0, 20.0, 21.0},
					{39.0 ,14.0 ,17.0, 18.0, 45.0, 24.0, 34.0, 27.0, 20.0,  0.0, 43.0},
					{10.0 ,32.0, 52.0, 42.0,  7.0, 51.0, 49.0, 15.0, 21.0, 43.0,  0.0}
					};
		KMeans km=new KMeans();
		km.dataset=marks;
		ArrayList<double[]> list=km.getRepresentationData(4);
		for(int i=0;i<list.size();i++)
		{
			for(int j=0;j<list.get(i).length;j++)
			{
				System.out.print(list.get(i)[j]);
				System.out.print(" ");
			}
			System.out.println();
		}
	}

	public void loadData(String fileName) {
		
	}
}
