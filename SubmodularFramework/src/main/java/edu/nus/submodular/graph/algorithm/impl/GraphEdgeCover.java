package edu.nus.submodular.graph.algorithm.impl;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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

public class GraphEdgeCover implements DataInterface{
	public Set<DefaultEdge> coveredEdge = new HashSet<DefaultEdge>();
	public Set<String> resultVertex = new HashSet<String>();
	UndirectedGraph<String, DefaultEdge> graph;
	public GraphEdgeCover()
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
				System.out.println(vertex[i]);
			}
			if(i!=0)
			{
				if(!vertex[0].equals(vertex[i]))
				{
					if(!graph.containsEdge(vertex[0], vertex[i]))
						graph.addEdge(vertex[0], vertex[i]);
				}
			}
		};
	}
	public void computeResult(int numOfElement,Set<String> candidateSet)
	{
		if(numOfElement==-1)
		{
			while(true)
			{
				boolean result=chooseBestOne(null);
				if(result==false)
					break;
			}
		}
		else
		{
			for(int i=0;i<numOfElement;i++)
			{
				boolean result=chooseBestOne(candidateSet);
				if(result==false)
					break;
			}
		}
	}
	public boolean chooseBestOne(Set<String> candidateSet)
	{
		Set<String> vertexset=graph.vertexSet();  //All the vertex in grpah
		int maximumBenefit=-1;
		String selectNode = null;  // final node to be selected
		Iterator<String> vertexIter;
		if(candidateSet==null)
			vertexIter=vertexset.iterator();  //get the iterator of vertex
		else
			vertexIter=candidateSet.iterator();
		while(vertexIter.hasNext())
		{
			String srcNode=vertexIter.next();   //check the source node
			int benefit=-1;  //calculate the benefit
			if(!resultVertex.contains(srcNode)) 
			{
				Set<DefaultEdge> outEdge=graph.edgesOf(srcNode); //get all edges out from
				Iterator<DefaultEdge> edgeIter=outEdge.iterator(); //iterator of the edges.
				while(edgeIter.hasNext())
				{
					DefaultEdge currentEdge=edgeIter.next();  //get the current edge
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
		for(Text data:values)
		{
			String[] vertex=data.toString().split(" |\\t");		
			for(int i=0;i<vertex.length;i++)
			{
				if(!graph.containsVertex(vertex[i]))
				{
					graph.addVertex(vertex[i]);
				}
				if(i!=0&&!(vertex[0].equals(vertex[i])))
				{		
					graph.addEdge(vertex[0], vertex[i]);
					System.err.println(vertex[i]);
				}
			}
			System.err.println("kk");
		}
		Integer numOfElement=context.getConfiguration().getInt(Macros.NUMOFELEMENT, -1);
		computeResult(numOfElement,null);
		Iterator<String> resultIter=resultVertex.iterator();
		while(resultIter.hasNext())
		{
			String result=resultIter.next();
			_key.set(Macros.KEYORIGINAL);
			Text txt_result=new Text();
			txt_result.set(result);
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
	}
	public void reduceData(Text _key, Iterable<Text> values,
			org.apache.hadoop.mapreduce.Reducer.Context context){
		// TODO Auto-generated method stub
		Set<String> candidateSet=new HashSet<String>();
		for(Text data:values)
		{	
			candidateSet.add(data.toString());
		}
		String inputPath=context.getConfiguration().get(Macros.INPUTPATH);
		readSourceFile(inputPath);
		Integer numOfElement=context.getConfiguration().getInt(Macros.NUMOFELEMENT, -1);
		computeResult(numOfElement,candidateSet);
		Iterator<String> resultIter=resultVertex.iterator();
		Set<DefaultEdge> resultEdgeSet=new HashSet<DefaultEdge>();
		while(resultIter.hasNext())
		{
			String result=resultIter.next();
			resultEdgeSet.addAll(graph.edgesOf(result));
			_key.set(Macros.KEYRESULT);
			Text txt_result=new Text();
			txt_result.set(result);
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
		_key.set("Covernum");
		try {
			context.write(_key, new Integer(resultEdgeSet.size()).toString());
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void readSourceFile(String path)
	{
		Path dcPath=new Path(path);
		Set<String> result=new HashSet<String>();
		Configuration conf=new Configuration();
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			if(!fs.isDirectory(dcPath))
			{
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(dcPath)));
				while (true) {
					String dataline=br.readLine();
					if(dataline==null)
						break;
					String originData=dataline.trim();
					this.addGraphData(originData);
				}	
			}
			else
			{
				FileStatus[] status=fs.listStatus(dcPath);
				for(int index=0;index<status.length;index++)
				{
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[index].getPath())));
					while (true) {
						String dataline=br.readLine();
						if(dataline==null)
							break;
						String originData=dataline.trim();
						this.addGraphData(originData);
					}	
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
