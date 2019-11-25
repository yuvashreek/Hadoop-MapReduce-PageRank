package cps534.asn2;


import java.util.ArrayList;
import java.util.List;

public class Node {
	private String id; // id of the node
	private Double pageRank; // distance of the node from the source
	private List<String> adjList = null; // list of edges
	private boolean isNode = true;

	public Node() // initialize a empty node
	{

	}

	public Node(String nodeInfo) {
		String[] inputLine = nodeInfo.split("\t");
		if (inputLine.length > 1) {
			this.id = inputLine[0];
			this.pageRank = Double.parseDouble(inputLine[1]);
			String[] parentnodes = inputLine[2].split(",");
			adjList = new ArrayList<String>(parentnodes.length);
			for (String adjnode : parentnodes) {
				this.adjList.add(adjnode);
			}
			this.isNode = true;
		} else {
			this.isNode = false;
			this.pageRank = Double.parseDouble(inputLine[0]);
		}

	}

	public String getNodeInfo() {
		StringBuilder s = new StringBuilder();
		s.append(this.id).append("\t").append(this.pageRank).append("\t");
		for (String adjnode : adjList) {
			s.append(adjnode).append(",");
		}
		return s.toString();

	}

	public boolean isNode() {
		return isNode;
	}

	public void setNode(boolean isNode) {
		this.isNode = isNode;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Double getPageRank() {
		return pageRank;
	}

	public void setPageRank(Double pageRank) {
		this.pageRank = pageRank;
	}

	public List<String> getAdjList() {
		return adjList;
	}

	public void setAdjList(List<String> adjList) {
		this.adjList = adjList;
	}

}