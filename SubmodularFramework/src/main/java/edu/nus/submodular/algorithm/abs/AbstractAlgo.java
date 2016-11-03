package edu.nus.submodular.algorithm.abs;

import java.util.ArrayList;

public class AbstractAlgo {
	protected ArrayList<double[]> dataset;
	protected ArrayList<Integer> representations;
	public ArrayList<double[]> getDataset() {
		return dataset;
	}
	public void setDataset(ArrayList<double[]> dataset) {
		this.dataset = dataset;
	}
	
}
