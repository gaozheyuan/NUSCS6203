package test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import diversification.InverseDocumentFrequencyMap;
import diversification.TfIdfCosineSimilarity;

public class TfIdfSimilarityTest {

	public static void main(String[] args) {
		TfIdfCosineSimilarity tf = new TfIdfCosineSimilarity();
		Map<String, Double> idfMap = InverseDocumentFrequencyMap.readIdfMapfromFile("idf-map-soccer");
		List<String> keywords = Arrays.asList("amazing", "player", "definitely");
		List<String> sentences = new ArrayList<>();
		try {
			BufferedReader br = new BufferedReader(new FileReader("soccer-2014-07-0"));
			String line;
			int count = 0;
			while (count++ < 10) {
				line = br.readLine();
				sentences.add(line);
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (String sentence : sentences){
			System.out.println(tf.CosineSimilarity(keywords, sentence, idfMap));
		}
		
		for (String sentence_1 : sentences){
			for (String sentence_2 : sentences){
				System.out.println(tf.CosineSimilarity(sentence_1, sentence_2, idfMap));
			}
		}
	}

}