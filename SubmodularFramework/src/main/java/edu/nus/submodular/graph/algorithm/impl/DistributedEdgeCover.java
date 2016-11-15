package edu.nus.submodular.graph.algorithm.impl;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
		texKey.set(Macros.MAPKEY);
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
		Text textOriginalKey=new Text();
		textOriginalKey.set(Macros.KEYORIGINAL);
		for(Text data:values)
		{
			try {
				context.write(textOriginalKey, data);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
		int numOfElement=context.getConfiguration().getInt(Macros.NUMOFELEMENT, -1);
		computeResult(numOfElement);
		Iterator<String> resultIter=resultVertex.iterator();
		while(resultIter.hasNext())
		{
			String result=resultIter.next();
			Set<DefaultEdge> connectedEdges=graph.edgesOf(result);
			Iterator<DefaultEdge> edgeIter=connectedEdges.iterator();
			String writeString=new String(result);
			while(edgeIter.hasNext())
			{
				DefaultEdge edge=edgeIter.next();
				String node1=graph.getEdgeSource(edge);
				String node2=graph.getEdgeTarget(edge);
				if(!node1.equals(result))
					writeString=writeString+" "+node1;
				else
					writeString=writeString+" "+node2;  //combine the result together and output
			}
			try {
				Text txt_result=new Text();
				txt_result.set(writeString);
				Text key=new Text();
				key.set(Macros.KEYRESULT);
				context.write(key, txt_result);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public void reduceData(Text _key, Iterable<Text> values,
			org.apache.hadoop.mapreduce.Reducer.Context context){
		// TODO Auto-generated method stub
		if(_key.toString().equals(Macros.KEYRESULT))
		{
			graph=new SimpleGraph<String, DefaultEdge>(DefaultEdge.class);
			for(Text data:values)
			{
				String[] nodes=data.toString().split(" ");
				for(int i=1;i<nodes.length;i++)
				{
					if(!graph.containsVertex(nodes[0]))
						graph.addVertex(nodes[0]);
					if(!graph.containsVertex(nodes[i]))
						graph.addVertex(nodes[i]);
					if(!graph.containsEdge(nodes[0], nodes[i]))
						graph.addEdge(nodes[0], nodes[i]);
				}
			}
			int numOfElement=context.getConfiguration().getInt(Macros.NUMOFELEMENT, -1);
			computeResult(numOfElement);
			Iterator<String> resultIter=resultVertex.iterator();
			while(resultIter.hasNext())
			{
				String result=resultIter.next();
				Text txt_result=new Text();
				txt_result.set(result);
				System.err.println(result);
				try {
					context.write(_key, txt_result);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				FileSystem fs = FileSystem.get(context.getConfiguration());
				Path pt=new Path(Macros.ROUNDRESULTFILE);
				FSDataOutputStream out;
				out = fs.create(pt);
				out.writeBytes(new Integer(coveredEdge.size()).toString());
				out.flush();
				out.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		if(_key.toString().equals(Macros.KEYORIGINAL))
		{
			graph=new SimpleGraph<String, DefaultEdge>(DefaultEdge.class);
			for(Text data:values)
			{
				String[] nodes=data.toString().split(" ");
				for(int i=1;i<nodes.length;i++)
				{
					if(!graph.containsVertex(nodes[0]))
						graph.addVertex(nodes[0]);
					if(!graph.containsVertex(nodes[i]))
						graph.addVertex(nodes[i]);
					if(!graph.containsEdge(nodes[0], nodes[i]))
						graph.addEdge(nodes[0], nodes[i]);
				}
			}
			try {
				FileSystem fs = FileSystem.get(context.getConfiguration());
				Path pt=new Path(Macros.TARGETRESULTFILE);
				FSDataOutputStream out;
				out = fs.create(pt);
				out.writeBytes(new Integer(graph.edgeSet().size()).toString());
				out.flush();
				out.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}
}
