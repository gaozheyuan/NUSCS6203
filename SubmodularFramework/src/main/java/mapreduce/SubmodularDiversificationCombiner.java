package mapreduce;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import diversification.Document;
import diversification.GreedyDiversification;
import diversification.InverseDocumentFrequencyMap;
import interfaces.NMGreeDistCombiner;

public class SubmodularDiversificationCombiner extends NMGreeDistCombiner<Document> {
	private Logger logger = Logger.getLogger(SubmodularDiversificationCombiner.class);
	private final static IntWritable one = new IntWritable(1);

	@Override
	public void reduce(IntWritable inKey, Iterable<Text> inValue, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		Map<String, Double> idfMap = InverseDocumentFrequencyMap.readIdfMapfromHDFS(conf.get("idfMapFileName"), conf);
		int numOfKeywords = conf.getInt("NumOfKeywords", 1);
		List<String> keywords = new ArrayList<>();
		for (int i = 0; i < numOfKeywords; i++) {
			keywords.add(conf.get("Keyword" + i));
		}
		double lambda = conf.getDouble("lambda", 0.8d);
		int k = conf.getInt("k", 5);
		logger.info("idfMap size = " + idfMap.size());

		List<Document> documents = new ArrayList<>();
		for (Text text : inValue) {
			documents.add(new Document(0, text.toString()));
		}
		GreedyDiversification greedyDiversification = new GreedyDiversification(documents, keywords, idfMap, lambda);

		Set<Document> result_1 = greedyDiversification.greedySummarization(k);
		double score_1 = greedyDiversification.getScore();
		logger.info("result_1 size = " + result_1.size());
		greedyDiversification.documents.removeAll(result_1);
		greedyDiversification.setScore(0.0d);
		Set<Document> result_2 = greedyDiversification.greedySummarization(k);
		double score_2 = greedyDiversification.getScore();
		logger.info("result_2 size = " + result_2.size());

		this.outputResult(score_1, result_1, score_2, result_2, conf);
		
		for (Document doc : result_1) {
			context.write(one, new Text(doc.getSentence()));
		}
		for (Document doc : result_2) {
			context.write(one, new Text(doc.getSentence()));
		}
	}
	
	public void outputResult(double score_1, Set<Document> result_1, double score_2, Set<Document> result_2, Configuration conf) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		if (score_1 >= score_2) {
			Path filenamePath = new Path(conf.get("outputPath") + "/result-" + Double.toString(score_1));
			if (fs.exists(filenamePath)) {
				fs.delete(filenamePath, true);
			}

			OutputStream os = fs.create(filenamePath);
			BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
			br.write(Double.toString(score_1) + "\n");
			for (Document doc : result_1) {
				br.write(doc.getSentence().trim() + "\n");
			}
			br.close();
			os.close();
		} else {
			Path filenamePath = new Path(conf.get("outputPath") + "/result-"  + Double.toString(score_2));
			if (fs.exists(filenamePath)) {
				fs.delete(filenamePath, true);
			}

			OutputStream os = fs.create(filenamePath);
			BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
			br.write(Double.toString(score_2) + "\n");
			for (Document doc : result_2) {
				br.write(doc.getSentence().trim() + "\n");
			}
			br.close();
			os.close();
		}
		fs.close();
	}

}
