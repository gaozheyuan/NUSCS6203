package edu.nus.submodular.algorithm;

import java.util.ArrayList;

public interface InterfaceAlgo {
	//extract
	ArrayList<double[]> getRepresentationData(int numofData);
	double calculateTotalError(ArrayList<Integer> representations);
	int[] assignLabel(ArrayList<Integer> representations);
	int pickNextElement();
	double calculateTwoElementsError(double[] first, double[] second);
	void loadData(String fileName);
}
