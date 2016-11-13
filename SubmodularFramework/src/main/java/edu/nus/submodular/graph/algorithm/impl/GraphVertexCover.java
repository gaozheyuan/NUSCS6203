package edu.nus.submodular.graph.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.*;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
public class GraphVertexCover {
	public Set<String> coveredVertex = new HashSet<String>();
	public Set<String> resultVertex = new HashSet<String>();
	DirectedGraph<String, DefaultEdge> graph;
	public GraphVertexCover()
	{
		graph=new DefaultDirectedGraph<String, DefaultEdge>(DefaultEdge.class);
	}
	public void addGraphData(String line)
	{
		String[] vertex=line.split(" ");		
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
			if(!resultVertex.contains(srcNode)) 
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
			}
			if(benefit>maximumBenefit)
			{
				maximumBenefit=benefit;
				selectNode=srcNode;
			}
		}
		if(maximumBenefit==-1)
			return false;
		else
		{
			resultVertex.add(selectNode);
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
}
