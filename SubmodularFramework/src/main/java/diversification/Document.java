package diversification;

import java.util.Map;

public class Document implements Comparable<Document>{
	int id;
	String sentence;
	Map<String, Integer> wordsFrequency;
	double score;
	double temp_score;
	
	public Document(int id, String sentence){
		this.id = id;
		this.sentence = sentence;
		this.temp_score = 0.0d;
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getSentence() {
		return sentence;
	}
	public void setSentence(String sentence) {
		this.sentence = sentence;
	}
	public Map<String, Integer> getWordsFrequency() {
		return wordsFrequency;
	}
	public void setWordsFrequency(Map<String, Integer> wordsFrequency) {
		this.wordsFrequency = wordsFrequency;
	}
	public double getScore() {
		return score;
	}
	public void setScore(double score) {
		this.score = score;
	}

	@Override
	public int compareTo(Document o) {
		if (this.temp_score - o.temp_score < -1.0e6)
			return -1;
		else if (this.temp_score - o.temp_score > 1.0e6)
			return 1;
		else if (this.id != o.id)
			return this.id - o.id;
		else
			return this.sentence.compareTo(o.sentence);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		result = prime * result + ((sentence == null) ? 0 : sentence.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Document other = (Document) obj;
		if (id != other.id)
			return false;
		if (sentence == null) {
			if (other.sentence != null)
				return false;
		} else if (!sentence.equals(other.sentence))
			return false;
		return true;
	}

}
