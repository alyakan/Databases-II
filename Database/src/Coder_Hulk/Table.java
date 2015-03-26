package Coder_Hulk;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.logging.FileHandler;

import BTree.BTree;


public class Table {
	private String tableName;
	// indices: Stores Column Names with indices along with root Nodes to each BTree index created
	// on that column
	private Hashtable <String,BTree> indices; 
	private ArrayList<String> indexedCols; // Stores column names that are indexed
	private Pages pages; // TODO Save pages in a .class
	/* savedIndices: Each column name gets an arraylist of tuples (key,value) for storing indices
	 * This way we can rebuild the BTree. TODO This object will be paged
	 * Key = Column's value (eg. 'Aly' if column is name). Value = number of page where the index is stored*/
	private Hashtable<String,ArrayList<String>> savedIndices;
	
	public Table(String tableName) {
		this.tableName = tableName;
		indices = new Hashtable<String,BTree>();
		indexedCols = new ArrayList<String>();
		pages = new Pages(tableName);
		savedIndices = new Hashtable<String,ArrayList<String>>();
	}
	
	public void createIndexToSavedIndices(String colName) {
		savedIndices.put(colName, new ArrayList<String>()); // Adding a new indexed column
	}
	public void addToSavedIndices(String colName, String index) {
		savedIndices.get(colName).add(index); // Adding an index to an already existing indexed column
	}

	public Hashtable <String,BTree> getIndices() {
		return indices;
	}
	
	public void setIndices(Hashtable <String,BTree> indicies) {
		this.indices = indicies;
	}
	
	public void addToIndices(String colName, BTree root) {
		indices.put(colName, root);
	}

	public ArrayList<String> getIndexedCols() {
		return indexedCols;
	}

	public void setIndexedCols(ArrayList<String> indexedCols) {
		this.indexedCols = indexedCols;
	}

	public Pages getPages() {
		return pages;
	}

	public void setPages(Pages pages) {
		this.pages = pages;
	}

}
