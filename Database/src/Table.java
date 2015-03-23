import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.logging.FileHandler;

import BTree.BTree;


public class Table {
	private String tableName;
	private DBHandler handler;
	private Hashtable <String,BTree> indices; // Stores Column Names with indices along with root Nodes to each BTree index created
	// on that column
	private ArrayList<String> indexedCols;
	
	public Table(String tableName) {
		this.tableName = tableName;
		setHandler(new DBHandler(this.tableName));  // Initializes the handler
		indices = new Hashtable<String,BTree>();
		indexedCols = new ArrayList<String>();
	}

	public DBHandler getHandler() {
		return handler;
	}

	public void setHandler(DBHandler handler) {
		this.handler = handler;
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
}
