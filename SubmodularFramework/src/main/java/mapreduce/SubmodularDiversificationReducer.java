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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import diversification.Document;
import diversification.GreedyDiversification;
import diversification.InverseDocumentFrequencyMap;
import interfaces.NMGreeDistReducer;

public class SubmodularDiversificationReducer extends NMGreeDistReducer<Document> {

	private Logger logger = Logger.getLogger(SubmodularDiversificationReducer.class);

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

		logger.info("k=" + k);
		logger.info("idfMap.size()=" + idfMap.size());

		List<Document> documents = new ArrayList<>();
		for (Text text : inValue) {
			documents.add(new Document(0, text.toString()));
		}
		GreedyDiversification greedyDiversification = new GreedyDiversification(documents, keywords, idfMap, lambda);

		Set<Document> result_1 = greedyDiversification.greedySummarization(k);
		logger.info("result.size()=" + result_1.size());
		for (Document doc : result_1)
			logger.info(doc.getSentence());
		double score_1 = greedyDiversification.getScore();
		
		greedyDiversification.documents.removeAll(result_1);
		greedyDiversification.setScore(0.0d);
		Set<Document> result_2 = greedyDiversification.greedySummarization(k);
		double score_2 = greedyDiversification.getScore();
		
		if (score_1 >= score_2) {
			this.outputFinalResult(score_1, result_1, conf);
		} else {
			this.outputFinalResult(score_2, result_2, conf);
		}

		for (Document doc : result_1) {
			context.write(new Text(doc.getSentence()), new DoubleWritable(doc.getScore()));
		}
	}
	
	public void outputFinalResult(double score, Set<Document> result, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
//		FileStatus[] status = fs.listStatus(new Path(conf.get("outputPath")));
//		double temp = score;
//		String result_name = "";
//		for (int i=0;i<status.length;i++){
//			String filename = status[i].getPath().getName();
//			double temp_score = Double.parseDouble(filename.split("-")[1]);
//			if (temp_score > temp) {
//				temp = temp_score;
//				result_name = filename;
//			}
//		}
//		
//		if (!result_name.equals("")) {
//			Path filenamePath = new Path(conf.get("outputPath") + "/" + result_name);
//			Path new_filenamePath = new Path(conf.get("outputPath") + "/result-final");
//			
//			fs.rename(filenamePath, new_filenamePath);
//			return;
//		}
		
		Path filenamePath = new Path(conf.get("outputPath") + "/result-" + Double.toString(score));
		if (fs.exists(filenamePath)) {
			fs.delete(filenamePath, true);
		}

		OutputStream os = fs.create(filenamePath);
		BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
		br.write(Double.toString(score) + "\n");
		for (Document doc : result) {
			br.write(doc.getSentence().trim() + "\n");
		}
		br.close();
		os.close();
	}

}
