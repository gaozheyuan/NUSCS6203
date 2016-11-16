package edu.nus.submodular.clustering.algorithm.abst;

import java.util.ArrayList;

public class AbstractAlgo {
	protected ArrayList<double[]> dataset;
	protected ArrayList<double[]> representations;
	public ArrayList<double[]> getDataset() {
		return dataset;
	}
	public void setDataset(ArrayList<double[]> dataset) {
		this.dataset = dataset;
	}
	
}
