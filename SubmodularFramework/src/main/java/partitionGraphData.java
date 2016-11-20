import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jgrapht.UndirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;
public class partitionGraphData {
	public static void main(String[] args)
	{
		UndirectedGraph<String, DefaultEdge> graph=new SimpleGraph<String, DefaultEdge>(DefaultEdge.class);
		Path path=new Path("graphdata/CA-GrQc.txt");
		Configuration conf=new Configuration();
		Integer numofFiles=new Integer(8);
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
			while(true)
			{
				String value=br.readLine();
				if(value==null)
					break;
				String[] nodes=value.split(" |\\t");
				if(!nodes[0].equals(nodes[1]))
				{
					if(!graph.containsVertex(nodes[0]))
						graph.addVertex(nodes[0]);
					if(!graph.containsVertex(nodes[1]))
						graph.addVertex(nodes[1]);
					graph.addEdge(nodes[0], nodes[1]);
				}
			}
			Set<DefaultEdge> alledge=graph.edgeSet();
			int totalLines=alledge.size();
			String partitionName="partition"+numofFiles.toString()+"-";
			double partLine=(double)totalLines/numofFiles;
			int count=0;
			Set<String> nodeset=graph.vertexSet();
			Iterator<String> nodeiter=nodeset.iterator();
			while(nodeiter.hasNext())
			{
				String node=nodeiter.next();
				Set<DefaultEdge> edgeset=graph.edgesOf(node);
				Iterator<DefaultEdge> edgeiter=edgeset.iterator();
				Integer indexNum=(int)(count/partLine);
				while(edgeiter.hasNext())
				{
					DefaultEdge edge=edgeiter.next();
					String source=graph.getEdgeSource(edge);
					String target=graph.getEdgeTarget(edge);
					String fileName=partitionName+indexNum.toString()+".txt";
					File writeFile=new File(fileName);
					FileUtils.writeStringToFile(writeFile,source+"\t"+target+"\n", true);
				}
				count+=edgeset.size();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
