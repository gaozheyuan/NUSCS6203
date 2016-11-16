import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;


public class generateSample {
	public static void main(String[] args)
	{
		int numofFeatures=10;
		int numofElements=10000;
		File inputFile=new File("testinput.txt");
		try {
			for(int y=0;y<numofElements;y++)
			{
				for(int x=0;x<numofFeatures;x++)
				{
					Double data=Math.random();
					if(x<numofFeatures-1)
						FileUtils.writeStringToFile(inputFile,data.toString()+" ", true);
					else
						FileUtils.writeStringToFile(inputFile,data.toString(), true);
				}
				FileUtils.writeStringToFile(inputFile,"\n", true);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
}
