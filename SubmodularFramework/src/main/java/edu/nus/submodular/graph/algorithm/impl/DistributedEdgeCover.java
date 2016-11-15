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
import org.jgrapht.DirectedGraph;
import org.jgrapht.UndirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;

import edu.nus.submodular.datainterface.DataInterface;
import edu.nus.submodular.macros.Macros;

public class DistributedEdgeCover implements DataInterface{
	public Set<DefaultEdge> coveredEdge = new HashSet<DefaultEdge>();
	public Set<String> resultVertex = new HashSet<String>();
	UndirectedGraph<String, DefaultEdge> graph;
	public DistributedEdgeCover()
	{
		graph=new SimpleGraph<String, DefaultEdge>(DefaultEdge.class);
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
	
	public void computeResult(int numOfElement)
	{
		for(int index=0;index<numOfElement;index++)
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
			int benefit=-1;  //calculate the benefit
			if(!resultVertex.contains(srcNode)) 
			{
				benefit=0;
				Set<DefaultEdge> outEdge=graph.edgesOf(srcNode); //get all edges out from
				Iterator<DefaultEdge> edgeIter=outEdge.iterator(); //iterator of the edges.
				while(edgeIter.hasNext())
				{
					DefaultEdge currentEdge=edgeIter.next();  
					if(!(coveredEdge.contains(currentEdge)))					
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
			Set<DefaultEdge> outEdges=graph.edgesOf(selectNode);
			Iterator<DefaultEdge> edgeIter=outEdges.iterator();
			while(edgeIter.hasNext())
			{
				DefaultEdge currentEdge=edgeIter.next();
				coveredEdge.add(currentEdge);
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
	public void mapData(LongWritable ikey, Text ivalue, Context context) {
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
			{
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
		int numOfElement=context.getConfiguration().getInt(Macros.STRINGELEMENT, -1);
		computeResult(numOfElement);
		Iterator<String> resultIter=resultVertex.iterator();
		while(resultIter.hasNext())
		{
			System.out.println("size"+resultVertex.size());
			String result=resultIter.next();
			System.out.println("key "+_key+"result "+result);
			Text txt_result=new Text();
			txt_result.set(result);
			System.out.println(context);
			try {
				context.write(_key, txt_result);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("size"+resultVertex.size());
		}
	}
	public void reduceData(Text _key, Iterable<Text> values,
			org.apache.hadoop.mapreduce.Reducer.Context context){
		// TODO Auto-generated method stub
		for(Text data:values)
		{
			try {
				context.write(_key,data);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
