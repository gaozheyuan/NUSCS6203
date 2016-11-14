package edu.nus.submodular.graph.algorithm.impl;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.jgrapht.*;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import edu.nus.submodular.datainterface.DataInterface;
public class GraphVertexCover implements DataInterface{
	public Set<String> coveredVertex = new HashSet<String>();
	public Set<String> resultVertex = new HashSet<String>();
	DirectedGraph<String, DefaultEdge> graph;
	public GraphVertexCover()
	{
		graph=new DefaultDirectedGraph<String, DefaultEdge>(DefaultEdge.class);
	}
	public void addGraphData(String line)
	{
		String[] vertex=line.split(" |\\t");		
		for(int i=0;i<vertex.length;i++)
		{
			if(!graph.containsVertex(vertex[i]))
			{
				graph.addVertex(vertex[i]);
			}
			if(i!=0)
			{
				if(!graph.containsEdge(vertex[0], vertex[i]))
					graph.addEdge(vertex[0], vertex[i]);
			}
		};
	}
	public void computeResult()
	{
		while(true)
		{
			boolean result=chooseBestOne();
			if(result==false)
				break;
		}
	}
	public boolean chooseBestOne()
	{
		Set<String> vertexset=graph.vertexSet();  //All the vertex in grpah
		int maximumBenefit=-1;
		String selectNode = null;  // final node to be selected
		Iterator<String> vertexIter=vertexset.iterator();  //get the iterator of vertex
		while(vertexIter.hasNext())
		{
			String srcNode=vertexIter.next();   //check the source node
			int benefit=0;  //calculate the benefit
			if(!resultVertex.contains(srcNode)&&!coveredVertex.contains(srcNode)) 
			{
				Set<DefaultEdge> outEdge=graph.outgoingEdgesOf(srcNode); //get all edges out from
				Iterator<DefaultEdge> edgeIter=outEdge.iterator(); //iterator of the edges.
				while(edgeIter.hasNext())
				{
					DefaultEdge currentEdge=edgeIter.next();  //get the current edge
					String destNode=graph.getEdgeTarget(currentEdge); //get the destination node					
					if(!(coveredVertex.contains(destNode))&&!(resultVertex.contains(destNode)))					
					{									
						benefit++;	
					}
				}
				if(benefit>maximumBenefit)
				{
					maximumBenefit=benefit;
					selectNode=srcNode;
				}
			}
		}
		if(maximumBenefit==-1)
			return false;
		else
		{
			resultVertex.add(selectNode);
			System.out.println(selectNode);
			Set<DefaultEdge> outEdges=graph.outgoingEdgesOf(selectNode);
			Iterator<DefaultEdge> edgeIter=outEdges.iterator();
			while(edgeIter.hasNext())
			{
				DefaultEdge currentEdge=edgeIter.next();
				String destNode=graph.getEdgeTarget(currentEdge);	
				coveredVertex.add(destNode);
			}
			return true;
		}
	}
	public void outputResult()
	{
		Iterator<String> resultIter=resultVertex.iterator();
		while(resultIter.hasNext())
		{
			System.out.println(resultIter.next());
		}
	}
	public static void main(String[] args)
	{
		GraphVertexCover vc=new GraphVertexCover();
		try {
			BufferedReader br = new BufferedReader(new FileReader("file.txt"));
			String line=br.readLine();
			while(line!=null)
			{
				vc.addGraphData(line);
				line=br.readLine();
			}
			vc.computeResult();
			vc.outputResult();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void mapData(LongWritable ikey, Text ivalue, Context context) {
		// TODO Auto-generated method stub
		Text texKey = new Text();
		texKey.set("1");
		try {
			context.write(texKey, ivalue);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void combineData(Text _key, Iterable<Text> values,
			org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		for(Text data:values)
		{
			String[] vertex=data.toString().split(" |\\t");		
			for(int i=0;i<vertex.length;i++)
			{
				if(!graph.containsVertex(vertex[i]))
				{
					graph.addVertex(vertex[i]);
				}
				if(i!=0)
				{
					if(!graph.containsEdge(vertex[0], vertex[i]))
						graph.addEdge(vertex[0], vertex[i]);
				}
			};
		}
		computeResult();
		outputResult();
	}
	public void reduceData(Text _key, Iterable<Text> values,
			org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}
}
