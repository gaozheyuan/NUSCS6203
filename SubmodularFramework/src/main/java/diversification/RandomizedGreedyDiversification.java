package diversification;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

public class RandomizedGreedyDiversification extends GreedyDiversification {

	Random random = new Random();

	public RandomizedGreedyDiversification(List<Document> documents, List<String> keywords, Map<String, Double> idfMap,
			double lambda) {
		super(documents, keywords, idfMap, lambda);
	}

	public Set<Document> greedySummarization(int k) {
		Set<Document> candidate = new HashSet<>();
		for (int i = 0; i < k; i++) {
			PriorityQueue<Document> queue = new PriorityQueue<>(k);
			for (Document document : this.documents) {
				if (candidate.contains(document))
					continue;
				document.temp_score = this.getDeltaElement(candidate, document);
				if (queue.size() < k)
					queue.offer(document);
				else {
					if (queue.peek().temp_score < document.temp_score) {
						queue.poll();
						queue.offer(document);
					}
				}
			}

			List<Document> listRetrive = new ArrayList<>(queue);
			this.updateElement(candidate, listRetrive.get(this.random.nextInt(k)));
		}

		return candidate;
	}

}
