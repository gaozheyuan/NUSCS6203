package interfaces;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public abstract class NMGreeDistReducer<T> extends Reducer<IntWritable, Text, Text, DoubleWritable> {
	public abstract void outputFinalResult(double score, Set<T> result, Configuration conf) throws IOException;
}
