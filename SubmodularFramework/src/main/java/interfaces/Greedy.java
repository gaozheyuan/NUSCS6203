package interfaces;

import java.util.Set;

public interface Greedy<T> {
	public double getDeltaElement(Set<T> candidate, T new_document);
	public Set<T> greedySummarization(int k);
	public void updateElement(Set<T> candidate, T new_document);
}
