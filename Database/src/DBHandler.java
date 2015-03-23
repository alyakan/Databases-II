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
	private FileOutputStream fileOut;
	private ObjectOutputStream out;
	private FileInputStream fileIn;
	private ObjectInputStream in;
	
	public DBHandler(String tableName) {
		this.setTableName(tableName);
		pCount = 0;
		setrCount(0);
		setLimit(200); // For now
		String fileName = (tableName + pCount + ".class");
		createPage(fileName);
	}
	
	public void createPage(String fileName) {
		try {
			//writer = new FileWriter(fileName);
			fileOut = new FileOutputStream(fileName);
			out  = new ObjectOutputStream(fileOut);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void appendRecord(Hashtable <String,String> htblColNameValue) {
		try {
			out.writeObject(htblColNameValue);
			//out.close();
			//fileOut.close();
			//writer.append(record + "\n");
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
			createPage(fileName);
		}
		appendRecord(htblColNameValue);
		return pCount;
	}
	/* This method takes a table name (page of records) and loads that page into an ArrayList of Hashtables (records) 
	 * by de-serializing the page */
	public ArrayList<Hashtable<String,String>> loadRecordsFromPage(String ref) {
		ArrayList<Hashtable<String,String>> result = new ArrayList<Hashtable<String,String>>();
		try {
			fileIn = new FileInputStream(ref+".ser");
			in = new ObjectInputStream(fileIn);
			while(true) {
				result.add((Hashtable<String, String>) in.readObject());
			}
		} catch(IOException e) {
			
		} catch(ClassNotFoundException e) {
			
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
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

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}
	
	public static void main(String[]args) {
		Record r = new Record ("Lol");
		DBHandler handler = new DBHandler ("table");
		//handler.Insert(r);
		//handler.Insert(r);
		//handler.Insert(r);
	}

}
