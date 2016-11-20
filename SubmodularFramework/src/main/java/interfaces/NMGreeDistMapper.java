package interfaces;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public abstract class NMGreeDistMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
}
