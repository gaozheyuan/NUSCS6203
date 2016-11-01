package edu.nus.submodular.hadoop.core;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SubmodularMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Integer numOfGroups;
	private Integer currentNumOfLine=0;
	private Integer totalNumOfLine;
	public Integer getCurrentNumOfLine() {
		return currentNumOfLine;
	}
	public void setCurrentNumOfLine(Integer currentNumOfLine) {
		this.currentNumOfLine = currentNumOfLine;
	}
	public Integer getTotalNumOfLine() {
		return totalNumOfLine;
	}
	public void setTotalNumOfLine(Integer totalNumOfLine) {
		this.totalNumOfLine = totalNumOfLine;
	}
	public Integer getNumOfGroups() {
		return numOfGroups;
	}
	public void setNumOfGroups(Integer numOfGroups) {
		this.numOfGroups = numOfGroups;
	}
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		Text texKey = new Text();
		currentNumOfLine++;
		Integer groupIndex=(int)Math.floor((double)currentNumOfLine/totalNumOfLine*numOfGroups);
		texKey.set(groupIndex.toString());
		context.write(texKey, ivalue);
	}
}
