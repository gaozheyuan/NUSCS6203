package diversification;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DocumentWritable implements WritableComparable<DocumentWritable> {
	
	IntWritable id;
	Text sentence;
	
	public DocumentWritable(){
		this.id = new IntWritable();
		this.sentence = new Text();
	}
	
	public DocumentWritable(int id, String sentence){
		this.id = new IntWritable(id);
		this.sentence = new Text(sentence);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.id.readFields(in);
		this.sentence.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.id.write(out);
		this.sentence.write(out);
	}

	@Override
	public int compareTo(DocumentWritable o) {
		return this.id.get() - o.id.get();
	}

	public IntWritable getId() {
		return id;
	}

	public void setId(IntWritable id) {
		this.id = id;
	}

	public Text getSentence() {
		return sentence;
	}

	public void setSentence(Text sentence) {
		this.sentence = sentence;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.id.get();
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
		DocumentWritable other = (DocumentWritable) obj;
		if (this.id.get() != other.id.get())
			return false;
		return true;
	}

}
