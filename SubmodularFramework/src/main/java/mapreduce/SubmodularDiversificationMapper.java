package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import interfaces.NMGreeDistMapper;

public class SubmodularDiversificationMapper extends NMGreeDistMapper {

	private final static IntWritable one = new IntWritable(1);

	@Override
	public void map(LongWritable autoIncrKey, Text originalSentence, Context context)
			throws IOException, InterruptedException {
		context.write(one, originalSentence);
	}

}
