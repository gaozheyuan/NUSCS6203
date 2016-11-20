package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SubmodularDiversificationDriver {

	public static void main(String[] args) throws Exception {
		String dir = args[0];
		Configuration conf = new Configuration();
		conf.set("idfMapFileName", dir + "/idf-map-soccer");
		conf.setDouble("lambda", 0.8d);
		conf.set("inputPath", dir + "/input");
		conf.set("outputPath", dir + "/output");
		conf.setInt("k", Integer.parseInt(args[1]));
		int numofkeywords = Integer.parseInt(args[2]);
		conf.setInt("NumOfKeywords", numofkeywords);
		for (int i = 0; i < numofkeywords; i++) {
			conf.set("Keyword" + i, args[3 + i]);
		}

		// conf.set("Keyword" + 0, "messi");
		// conf.set("Keyword" + 1, "world");
		// conf.set("Keyword" + 2, "cup");

		Job job = Job.getInstance(conf, "Summarization");
		job.setJarByClass(SubmodularDiversificationDriver.class);
		job.setMapperClass(SubmodularDiversificationMapper.class);
		job.setCombinerClass(SubmodularDiversificationCombiner.class);
		job.setReducerClass(SubmodularDiversificationReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.setInputPaths(job, new Path(dir + "/input"));
		FileOutputFormat.setOutputPath(job, new Path(dir + "/output"));

		if (!job.waitForCompletion(true))
			return;
	}
}