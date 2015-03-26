package Coder_Hulk;
import java.io.*;
import java.util.ArrayList;
import java.util.Hashtable;
/* This class handles all entries and deletions to pages so we can control the Row Count in our pages and create new pages
 * when a page reaches the maximum count*/
public class DBHandler {
	private String tableName;
	private int pCount; // Page Count
	private int rCount; // Row Count
	private int limit; // Row Limit
	//private FileWriter writer;
	private FileOutputStream fileOut; // For pages
	private ObjectOutputStream out; // For pages
	private FileInputStream fileIn; // For pages
	private ObjectInputStream in; // For pages
	
	private ArrayList<ArrayList<Hashtable<String,String>>> listOfPages;
	private FileOutputStream listFileOut; // For list
	private ObjectOutputStream listOut; // For list
	private FileInputStream listFileIn; // For list
	private ObjectInputStream listIn; // For list
	
	public DBHandler(String tableName) {
		this.setTableName(tableName);
		pCount = 0;
		setrCount(0);
		setLimit(200); // For now
		String fileName = (tableName + pCount + ".class");
		listOfPages = new ArrayList<ArrayList<Hashtable<String,String>>>();
		listOfPages.add(new ArrayList<Hashtable<String,String>>());
		String listOfPagesFile = tableName + "arrayList.class";
		try {
			listFileOut = new FileOutputStream(listOfPagesFile, true); // Creating the listOfPages files
			fileOut = new FileOutputStream(fileName, true); // Creating a new page
			out  = new ObjectOutputStream(fileOut);
			fileIn = new FileInputStream(fileName);
			in = new ObjectInputStream(fileIn);
		} catch (IOException e) {
			e.printStackTrace();
		}
		//createPage(fileName);
	}
	
	public void createPage(String fileName) {
		try {
			fileOut = new FileOutputStream(fileName, true);
			out  = new ObjectOutputStream(fileOut);
			listOfPages.add(new ArrayList<Hashtable<String,String>>());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void appendRecord(Hashtable <String,String> htblColNameValue) {
		try {
			out.writeObject(htblColNameValue);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/* Insert a record into a page and return the current page count to be inserted into the B+Tree
	 * Create a new page if current page reached maximum row count */
	public int Insert(Hashtable <String,String> htblColNameValue) {
		rCount ++;
		if (rCount > limit) { // Create new page
			rCount = 0; // Reset row count
			pCount ++;
			String fileName = (tableName + pCount + ".class");
			try { // Making sure that the output stream is using the latest created page in the database
				fileOut = new FileOutputStream(fileName, true);
				out  = new ObjectOutputStream(fileOut);
			} catch (IOException io) {
			}
			createPage(fileName);
		}
		listOfPages.get(pCount).add(htblColNameValue); // Adding records to listOfPages TODO Update the file containing the array list
		appendRecord(htblColNameValue);
		return pCount;
	}
	/* This method takes a table name (page of records) and loads that page into an ArrayList of Hashtables (records) 
	 * by de-serializing the page */
	public ArrayList<Hashtable<String,String>> loadRecordsFromPage(String ref) { // TODO change ref from calling method to int index
		ArrayList<Hashtable<String,String>> result = new ArrayList<Hashtable<String,String>>();
		int index = Integer.parseInt(ref);
		result = listOfPages.get(index);
		return result;
	}
	
	/* This method deletes a file using the variable ref (File's name) */
	public void deleteFile(String ref) {
		try {
			if(!ref.contains(".class")) {
				ref += ".class";
			}
			File file = new File(ref);
			if (file.delete())
				System.out.println(ref);
		} catch (Exception e) {
			
		}
	}
	
	/* Deletes a record from listOfPages and updates required pages */
	public void deleteRecordFromList(String ref, Hashtable<String,String> record) {
		String recordString = record.toString();
		int i = Integer.parseInt(ref);
		ArrayList<Hashtable<String,String>> page = listOfPages.get(i);
		for(int j = 0; j < page.size(); j++) {
			String tempRecord = page.get(j).toString();
			if(tempRecord.equals(recordString)) {
				page.remove(j); // TODO UPDATE page of listOfPages and normal page
				break;
			}
		}
	}
	
	public void writeRecordsToPage(String ref, ArrayList<Hashtable<String,String>> records) {
		String fileName = (ref + ".class");
		createPage(fileName);
		for(int i = 0; i < records.size(); i++) {
			appendRecord(records.get(i));
		}
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getrCount() {
		return rCount;
	}

	public void setrCount(int rCount) {
		this.rCount = rCount;
	}
	
	public int getpCount() {
		return pCount;
	}

	public void setpCount(int pCount) {
		this.pCount = pCount;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}
	
	public static void main(String[]args) {
		
	}
	
	public ArrayList<ArrayList<Hashtable<String,String>>> getListOfPages() {
		return listOfPages;
	}

	public void setListOfPages(ArrayList<ArrayList<Hashtable<String,String>>> listOfPages) {
		this.listOfPages = listOfPages;
	}

}
