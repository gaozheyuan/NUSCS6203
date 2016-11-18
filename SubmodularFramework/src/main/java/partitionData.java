import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class partitionData {
	public static void main(String[] args)
	{
		Path path=new Path("testinput100000.txt");
		Configuration conf=new Configuration();
		Integer numofFiles=new Integer(8);
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
			int totalLines=0;
			while(true)
			{
				String value=br.readLine();
				totalLines++;
				if(value==null)
					break;
			}
			br=new BufferedReader(new InputStreamReader(fs.open(path)));
			String partitionName="partition8-";
			int count=0;
			double partLine=(double)totalLines/numofFiles;
			while(true)
			{
				String value=br.readLine();
				Integer indexNum=(int)(count/partLine);
				String fileName=partitionName+indexNum.toString()+".txt";
				File writeFile=new File(fileName);
				FileUtils.writeStringToFile(writeFile,value+"\n", true);
				count++;
				if(count==totalLines-1)
					break;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
