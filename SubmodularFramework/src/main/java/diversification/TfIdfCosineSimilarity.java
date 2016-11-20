package diversification;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class TfIdfCosineSimilarity {

	public Map<String, Integer> getTermFrequency(String sentence) {
		Map<String, Integer> tfMap = new TreeMap<>();
		String[] split = sentence.toLowerCase().split("\\s+");
		for (String token : split) {
			if (tfMap.containsKey(token))
				tfMap.replace(token, tfMap.get(token) + 1);
			else
				tfMap.put(token, 1);
		}
		return tfMap;
	}

	public double CosineSimilarity(List<String> keywords, String sentence, Map<String, Double> idfMap) {
		Map<String, Integer> tfMap = getTermFrequency(sentence);

		Map<String, Double> vector_keywords = new TreeMap<>();
		for (String keyword : keywords) {
			if (idfMap.containsKey(keyword))
				vector_keywords.put(keyword, idfMap.get(keyword));
		}

		Map<String, Double> vector_sentense = new TreeMap<>();
		for (Entry<String, Integer> term : tfMap.entrySet()) {
			if (idfMap.containsKey(term.getKey()))
				vector_sentense.put(term.getKey(), term.getValue() * idfMap.get(term.getKey()));
		}

		return getVectorProduct(vector_keywords, vector_sentense)
				/ (getVectorLength(vector_keywords) * getVectorLength(vector_sentense));
	}

	public double CosineSimilarity(String sentence_1, String sentence_2, Map<String, Double> idfMap) {
		Map<String, Integer> tfMap_1 = getTermFrequency(sentence_1);
		Map<String, Integer> tfMap_2 = getTermFrequency(sentence_2);

		Map<String, Double> vector_1 = new TreeMap<>();
		for (Entry<String, Integer> term : tfMap_1.entrySet()) {
			if (idfMap.containsKey(term.getKey()))
				vector_1.put(term.getKey(), term.getValue() * idfMap.get(term.getKey()));
		}

		Map<String, Double> vector_2 = new TreeMap<>();
		for (Entry<String, Integer> term : tfMap_2.entrySet()) {
			if (idfMap.containsKey(term.getKey()))
				vector_2.put(term.getKey(), term.getValue() * idfMap.get(term.getKey()));
		}

		return getVectorProduct(vector_1, vector_2) / (getVectorLength(vector_1) * getVectorLength(vector_2));
	}

	double getVectorProduct(Map<String, Double> vector_1, Map<String, Double> vector_2) {
		double product = 0.0d;
		for (Entry<String, Double> entry : vector_1.entrySet()) {
			if (vector_2.containsKey(entry.getKey()))
				product += entry.getValue() * vector_2.get(entry.getKey());
		}

		return product;
	}

	double getVectorLength(Map<String, Double> vector) {
		double length = 0.0d;
		for (double weight : vector.values()) {
			length += weight * weight;
		}

		return Math.sqrt(length);
	}
}
