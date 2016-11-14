package edu.nus.submodular.datainterface;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public interface DataInterface {
	public void mapData(LongWritable ikey, Text ivalue, org.apache.hadoop.mapreduce.Mapper.Context context);
	public void combineData(Text _key, Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException;
	public void reduceData(Text _key, Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException;
}
