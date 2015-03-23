import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.io.*;
import java.net.URL;

import BTree.BTree;

public class DBApp implements java.io.Serializable {
	static String fileName = "metadata.csv";
	static FileWriter writer;
	BufferedReader read;
	Hashtable <String,Table>tables;
	FileOutputStream fileOut; // Create a page for indices.. OutputStream for indices
	ObjectOutputStream out;
	
	public void init( ){
		// Populate tables
		try {
			writer = new FileWriter(fileName, true);
			//writer.append("Table Name, Column Name, Column Type, Key, Indexed, References" + "\n");
			writer.flush();
			read = new BufferedReader(new FileReader (fileName));
	        String line = "";
	        while ((line = read.readLine()) != null) {
	        	System.out.println(line);
	        }
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error");
		}
	}
	
	public void createTable(String strTableName, 
			Hashtable<String,String> htblColNameType, 
			Hashtable<String,String>htblColNameRefs, 
			String strKeyColName) throws DBAppException{
		// Enter data to MetaData File
		try {
		String record = ""; // Final record that goes into file
		
		String colNameType = htblColNameType.toString(); // Convert hash table to string
		String []colNameTypeArray = colNameType.split(", "); // Split the string by , to get pairs of Key=Value
		int size = colNameTypeArray.length;
		colNameTypeArray[0] = colNameTypeArray[0].substring(1); // Remove first bracket in string
		colNameTypeArray[size - 1] = colNameTypeArray[size - 1].substring(0,colNameTypeArray[size - 1].length() - 1); // Remove last bracket in string
		
		String colNameRefs = htblColNameRefs.toString();
		String []colNameRefsArray = colNameRefs.split(", ");
		int size2 = colNameRefsArray.length;
		colNameRefsArray[0] = colNameRefsArray[0].substring(1);
		colNameRefsArray[size2 - 1] = colNameRefsArray[size2 - 1].substring(0,colNameRefsArray[size2 - 1].length() - 1);
		
		//FileWriter writer = new FileWriter(fileName);
		
		for (int i = 0; i < size; i++) {
			record = "";
			record += strTableName + ", ";
			String []oneColNameType = colNameTypeArray[i].split("="); // Split each Key=Value by =
			String colName = oneColNameType[0]; // Get Column Name
			String colType = oneColNameType[1]; // Get Column Type
			record += colName + ", " + colType + ", ";
			if (strKeyColName.equals(colName)) { // Check if Column is a primary key
				record += "True, True, ";
				createIndex(strTableName, strKeyColName);
			} else {
				record += "False ,False, ";
			}
			boolean ref = false;
			for (int j = 0; j < size2 && !ref; j++) {
				String []oneColNameRefs = colNameRefsArray[j].split("=");
				String colName2 = oneColNameRefs[0];
				String colRefs = oneColNameRefs[1];
				if (colName2.equals(colName)) { // Check if Column references another table
					record += colRefs;
					ref = true;
				}
			}
			if (!ref) {
				record += "null";
			}
			record += "\n";
			writer.append(record);
			System.out.print(record);
		}
		} catch(IOException e) {
			
		}
		
	}
	
	public void createIndex(String strTableName,
			String strColName)
			throws DBAppException{
		BTree root = new BTree();
		tables.get(strTableName).addToIndices(strColName,root);
		tables.get(strTableName).getIndexedCols().add(strColName);
		String file = strTableName + strColName + ".class";
		try {
			fileOut = new FileOutputStream(file); // Create a page for indices
			//ObjectOutputStream out = new ObjectOutputStream(fileOut);
			//out.writeObject(record);
		} catch(IOException e) {
			
		}
	}
	
	public void insertIntoTable(String strTableName,
			Hashtable<String,String> htblColNameValue)
			throws DBAppException{
		Table temp = tables.get(strTableName);
		int pCount = temp.getHandler().Insert(htblColNameValue);
		Hashtable<String,BTree> indexedCols = temp.getIndices(); // Getting Indexed columns with their BTrees
		for(int i = 0; i < temp.getIndexedCols().size(); i++) { // For loop to insert values of indexed columns into BTrees
			String key = temp.getIndexedCols().get(i); // One of the indexed columns' names
			BTree<String, String> temproot = indexedCols.get(key); // Acquiring the BTree for the column
			temproot.insert(htblColNameValue.get(indexedCols.get(key)), "" + strTableName + pCount); // Insert into BTree. Key = Column's value. Value = tableName + pageCount (Reference)
			String file = strTableName + key + ".class";
			try {
				fileOut = new FileOutputStream(file); // Create a page for indices
				out = new ObjectOutputStream(fileOut);
				String record = htblColNameValue.get(indexedCols.get(key)) + "," + strTableName + pCount;
				out.writeObject(record);
			} catch(IOException e) {
				
			}
			
			
		}
	}
	
	public void deleteFromTable(String strTableName,
			Hashtable<String,String> htblColNameValue,
			String strOperator)
			throws DBEngineException{}
	
	public Iterator<?> selectValueFromTable(String strTable,
			Hashtable<String,String> htblColNameValue,
			String strOperator)
			throws DBEngineException{
				return null;
	}
	
	public Iterator<?> selectRangeFromTable(String strTable,
			Hashtable<String,String> htblColNameRange,
			String strOperator)
			throws DBEngineException{
				return null;}
	
	public void saveAll( ) throws DBEngineException{}
	
	public static void main(String[]args) {
		  Hashtable<String, String> balance = new Hashtable<String, String>();
	      Enumeration names;
	      //String str;
	      //double bal;

	      balance.put("Zara", "1");
	      balance.put("Mahnaz", "2");
	      balance.put("Ayan", "3");
	      balance.put("Daisy", "4");
	      balance.put("Qadir", "5");
		DBApp a = new DBApp();
		a.init();
		System.out.println("lol");
		names = balance.keys();
		String []b = null;
		Set<String> indexedColNames = balance.keySet();
		
		  System.out.println(balance.keySet().iterator().next());

		  System.out.println(balance.keySet().remove(balance.keySet().iterator().next()));
		  
		  System.out.println(balance.keySet().iterator().next());
	      
	}

}
