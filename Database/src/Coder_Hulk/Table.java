package Coder_Hulk;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
	
	public void addIndexToSavedIndices(String colName, String index) {
		savedIndices.get(colName).add(index); // Adding an index to an already existing indexed column
	}
	
	public void saveIndices() throws IOException{
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(tableName + "Indices.class")));
		oos.writeObject(savedIndices);
		oos.close();
	}
	
	public Hashtable<String,ArrayList<String>> loadIndices() throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(tableName + "Indices.class")));
		Hashtable<String,ArrayList<String>> result = (Hashtable<String,ArrayList<String>>)ois.readObject();
		ois.close();
		return result;
	}
	
	public void deleteIndices() throws IOException {
		File file = new File(tableName + "Indices.class");
		if(file.exists())
			file.delete();
	}
	
	public void savePages() throws IOException{
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(tableName + "Pages.class")));
		oos.writeObject(pages);
		oos.close();
	}
	
	public Pages loadPages() throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(tableName + "Pages.class")));
		Pages result = (Pages)ois.readObject();
		ois.close();
		return result;
	}
	
	public void deletePages() throws IOException {
		File file = new File(tableName + "Pages.class");
		if(file.exists())
			file.delete();
	}
	
	public void updatePages() throws IOException {
		deletePages();
		savePages();
	}
	
	public void updateIndices() throws IOException {
		deleteIndices();
		saveIndices();
	}
	
	/* After a deletion, an update for savedIndices is required to remove the deleted record indices */
	public void updateSavedIndices() { 
		savedIndices = new Hashtable<String,ArrayList<String>>(); // Re-initialize indices
		for(int i = 0; i < indexedCols.size(); i++) {
			ArrayList<String> tempList = new ArrayList<String>();
			String indexedCol = indexedCols.get(i); // Get the first indexed column name
			for(int j = 0; j <= pages.getpCount(); j++) { // Loop on all pages
				ArrayList<Hashtable<String,String>> onePage = pages.getpagesList().get(j); // Get one of the pages
				int currentPage = j; // current page's number
				for (int k = 0; k < onePage.size(); k++) { // Loop on all records inside that page
					String val = onePage.get(k).get(indexedCol); // Get the indexed column's value from a record
					String key = val + "," + j; // Create the tuple (value, pageNumber)
					tempList.add(key);
				}
			}
			savedIndices.put(indexedCol, tempList);
		}
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
	
	public Hashtable<String,ArrayList<String>> getSavedIndices() {
		return this.savedIndices;
	}

}
