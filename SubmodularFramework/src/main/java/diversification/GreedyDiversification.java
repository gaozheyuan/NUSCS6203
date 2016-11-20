package diversification;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import interfaces.Greedy;

public class GreedyDiversification implements Greedy<Document> {
	public List<Document> documents;
	Map<String, Double> idfMap;
	List<String> keywords;
	TfIdfCosineSimilarity similarityMeasure;
	double lambda;
	double score;

	public GreedyDiversification(List<Document> documents, List<String> keywords, Map<String, Double> idfMap, double lambda) {
		this.documents = documents;
		this.keywords = keywords;
		this.idfMap = idfMap;
		this.similarityMeasure = new TfIdfCosineSimilarity();
		this.lambda = lambda;
		this.score = 0.0d;
	}

	public double getDeltaElement(Set<Document> candidate, Document new_document) {
		double gain = this.similarityMeasure.CosineSimilarity(this.keywords, new_document.sentence, this.idfMap);

		double max_similarity = 0.0d;
		double decrease = 0.0d;
		for (Document doc : candidate) {
			double similarity = this.similarityMeasure.CosineSimilarity(doc.sentence, new_document.sentence,
					this.idfMap);
			if (similarity > max_similarity)
				max_similarity = similarity;
			if (similarity > doc.score)
				decrease += (similarity - doc.score);
		}

		return this.lambda * gain - (1.0d - this.lambda) * (max_similarity + decrease);
	}

	public void updateElement(Set<Document> candidate, Document new_document) {
		this.score += this.lambda
				* this.similarityMeasure.CosineSimilarity(this.keywords, new_document.sentence, this.idfMap);

		new_document.score = 0.0d;
		double decrease = 0.0d;
		for (Document doc : candidate) {
			double similarity = this.similarityMeasure.CosineSimilarity(doc.sentence, new_document.sentence,
					this.idfMap);
			if (new_document.score < similarity)
				new_document.score = similarity;
			if (doc.score < similarity) {
				decrease += (similarity - doc.score);
				doc.score = similarity;
			}
		}

		candidate.add(new_document);

		this.score -= (1.0d - this.lambda) * new_document.score;
		this.score -= (1.0d - this.lambda) * decrease;
	}

	public Set<Document> greedySummarization(int k) {
		Set<Document> candidate = new HashSet<>();
		for (int i = 0; i < k; i++) {
			double maxGain = 0.0d;
			Document nextDoc = null;
			for (Document document : this.documents) {
				if (candidate.contains(document))
					continue;
				double gain = this.getDeltaElement(candidate, document);
				if (gain > maxGain) {
					maxGain = gain;
					nextDoc = document;
				}
			}

			if (maxGain > 0 && nextDoc != null)
				this.updateElement(candidate, nextDoc);
			else
				break;
		}

		return candidate;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}
}
