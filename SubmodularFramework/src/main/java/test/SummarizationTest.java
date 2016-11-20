package test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import diversification.Document;
import diversification.GreedyDiversification;
import diversification.InverseDocumentFrequencyMap;
import diversification.RandomizedGreedyDiversification;

public class SummarizationTest {

	public static void main(String[] args) {
		List<Document> documents = new ArrayList<>();
		try {
			BufferedReader br = new BufferedReader(new FileReader("soccer-2014-07-0"));
			String line;
			int count = 0;
			while ((line = br.readLine()) != null) {
				Document doc = new Document(count++, line);
				documents.add(doc);
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<String> keywords = Arrays.asList("messi", "world", "cup");
		Map<String, Double> idfMap = InverseDocumentFrequencyMap.readIdfMapfromFile("idf-map-soccer");
		GreedyDiversification greedyDiversification = new GreedyDiversification(documents, keywords, idfMap, 0.5d);
		long t1 = System.currentTimeMillis();
		Set<Document> results = greedyDiversification.greedySummarization(5);
		long t2 = System.currentTimeMillis();
		System.out.println(t2 - t1);
		System.out.println(greedyDiversification.getScore());
		for (Document result : results) {
			System.out.println(result.getId() + "," + result.getScore() + "," + result.getSentence());
		}

		RandomizedGreedyDiversification randomizedGreedyDiversification = new RandomizedGreedyDiversification(documents,
				keywords, idfMap, 0.5d);
		long t3 = System.currentTimeMillis();
		Set<Document> randomized_results = randomizedGreedyDiversification.greedySummarization(5);
		long t4 = System.currentTimeMillis();
		System.out.println(t4 - t3);
		System.out.println(randomizedGreedyDiversification.getScore());
		for (Document result : randomized_results) {
			System.out.println(result.getId() + "," + result.getScore() + "," + result.getSentence());
		}
	}

}
