package interfaces;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public abstract class NMGreeDistCombiner<T> extends Reducer<IntWritable, Text, IntWritable, Text> {
	public abstract void outputResult(double score_1, Set<T> result_1, double score_2, Set<T> result_2, Configuration conf) throws IOException;
}
