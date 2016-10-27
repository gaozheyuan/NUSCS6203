package edu.nus.submodular.algorithm;

import java.util.ArrayList;

public class AbstractAlgo {
	protected double [][] dataset;
	protected ArrayList<Integer> representations;
	public double[][] getDataset() {
		return dataset;
	}

	public void setDataset(double[][] dataset) {
		this.dataset = dataset;
	}
}
