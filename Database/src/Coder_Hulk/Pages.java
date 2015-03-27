package Coder_Hulk;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;

public class Pages implements Serializable {
	private ArrayList<ArrayList<Hashtable<String,String>>> pagesList; // Array list containing pages of records, each page
	// is an array list of hash tables (records)
	private int pCount; // Number of pages created
	private String tableName;
	
	public Pages(String tableName) {
		this.tableName = tableName;
		pCount = 0;
		pagesList = new ArrayList<ArrayList<Hashtable<String,String>>>();
		addNewPage();
		pCount --;
	}
	
	public void addNewPage() {
		pagesList.add(new ArrayList<Hashtable<String,String>>()); // create a new page inside the array of pages
		pCount ++;
	}
	
	public int addRecord(Hashtable<String,String> record) {
		if (pCount > 200) { // Upon reaching a page's limit, create a new page
			addNewPage();
		}
		pagesList.get(pCount).add(record); // Add a record to the latest page
		return pCount;
	}
	
	public ArrayList<ArrayList<Hashtable<String,String>>> getpagesList() {
		return this.pagesList;
	}
	
	public int getpCount() {
		return this.pCount;
	} 
	
	public String gettableName() {
		return this.tableName;
	}
}
