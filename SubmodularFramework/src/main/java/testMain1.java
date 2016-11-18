import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class testMain1 {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		combineTwoFiles("2.txt","3.txt",conf);
	}
	public static void combineTwoFiles(String dcFile,String gdFile, Configuration conf)
	{
		String a="a";
		String b="b";
		System.out.println(a==b);
/*		Path dcPath=new Path(dcFile);
		Path gdPath=new Path(gdFile);
		Set<String> result=new HashSet<String>();
		try {
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(gdPath)));
			while(true)
			{
				String node=br.readLine();
				if(node==null)
					break;
				result.add(node.trim());
			}
			BufferedReader br2=new BufferedReader(new InputStreamReader(fs.open(dcPath)));
			while(true)
			{
				String node=br2.readLine();
				if(node==null)
					break;
				result.add(node.trim());
			}
			br2.close();
			FSDataOutputStream out=fs.create(dcPath);
			Iterator<String> resultIter=result.iterator();
			while(resultIter.hasNext())
			{
				String output=resultIter.next();
				out.writeBytes(output);
				System.out.println(output);
				out.writeChar('\n');
			}
			out.flush();
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}
}
