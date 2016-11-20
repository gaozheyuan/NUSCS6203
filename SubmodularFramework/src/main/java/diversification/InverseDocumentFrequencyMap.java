package diversification;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.TreeMap;
import java.util.TreeSet;

/**
 * @author Wang Yanhao Inverse Document Frequency for the whole corpus
 */
public class InverseDocumentFrequencyMap {

	public void buildIdfMap(File[] files, String output) {
		Map<String, Double> idfMap = new TreeMap<>();
		BufferedReader br = null;
		try {
			int count = 0;
			for (File file : files) {
				br = new BufferedReader(new FileReader(file));
				String cur_line;
				while ((cur_line = br.readLine()) != null) {
					String[] split = cur_line.toLowerCase().split("\\s+");
					count++;
					TreeSet<String> temp = new TreeSet<>();
					for (String token : split) {
						if (token.trim().length() <= 1)
							continue;
						temp.add(token.trim());
					}
					for (String t : temp) {
						if (idfMap.containsKey(t.trim())) {
							idfMap.replace(t.trim(), idfMap.get(t.trim()) + 1.0d);
						} else {
							idfMap.put(t.trim(), 1.0d);
						}
					}
				}
				br.close();
			}
			for (String key : idfMap.keySet()) {
				double frequency = idfMap.get(key);
				idfMap.put(key, 1.0d + Math.log(count / frequency));
			}

			FileWriter fw = new FileWriter(output);
			for (Entry<String, Double> entry : idfMap.entrySet()) {
				fw.write(entry.getKey() + " " + entry.getValue() + "\n");
			}
			fw.flush();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null)
				br = null;
		}
	}

	public static Map<String, Double> readIdfMapfromFile(String filename) {
		Map<String, Double> idfMap = new TreeMap<>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(filename));
			String cur_line;
			while ((cur_line = br.readLine()) != null) {
				String[] split = cur_line.split("\\s+");
				idfMap.put(split[0], Double.parseDouble(split[1]));
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null)
				br = null;
		}

		return idfMap;
	}

	public static Map<String, Double> readIdfMapfromHDFS(String filename, Configuration conf) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);

		Path path = new Path(filename);
		if (!fileSystem.exists(path)) {
			System.out.println("File " + filename + " does not exists");
			return null;
		}

		Map<String, Double> idfMap = new TreeMap<>();
		BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
		String cur_line;
		while ((cur_line = br.readLine()) != null) {
			String[] split = cur_line.split("\\s+");
			idfMap.put(split[0], Double.parseDouble(split[1]));
		}
		br.close();
		if (br != null)
			br = null;
		fileSystem.close();

		return idfMap;
	}

	public static void main(String[] args) {
		File dir = new File(".");
		FilenameFilter filter = new FilenameFilter() {
			public boolean accept(File directory, String fileName) {
				return fileName.startsWith("soccer");
			}
		};
		File[] files = dir.listFiles(filter);
		InverseDocumentFrequencyMap tfm = new InverseDocumentFrequencyMap();
		tfm.buildIdfMap(files, "idf-map-soccer");

		System.out.println(InverseDocumentFrequencyMap.readIdfMapfromFile("idf-map-soccer").size());
	}

}