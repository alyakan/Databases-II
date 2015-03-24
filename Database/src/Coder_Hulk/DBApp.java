package Coder_Hulk;
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
	boolean initializedMetadata = false;
	
	public void init( ){
		// Populate tables
		try {
			tables = new Hashtable<String,Table>();
			writer = new FileWriter(fileName, true);
			if(!initializedMetadata) {
				writer.append("Table Name, Column Name, Column Type, Key, Indexed, References" + "\n");
				writer.flush();
				initializedMetadata = true;
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
		Table table = new Table(strTableName);
		tables.put(strTableName, table);
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
			writer.flush();
			//System.out.print(record);
		}
		} catch(IOException e) {
			
		}
		
	}
	
	public ArrayList<String> htblKeys(Hashtable<String,String> htbl) {
		int length = htbl.toString().length();
		String temp = htbl.toString().substring(1, length - 1);
		String [] tempArray = temp.split(", ");
		ArrayList<String> keys = new ArrayList<String>();
		for(int i = 0; i < tempArray.length; i++) {
			String [] tempArray2 = tempArray[i].split("=");
			keys.add(tempArray2[0]);
		}
		return keys;
	}
	
	public ArrayList<String> htblValues(Hashtable<String,String> htbl) {
		int length = htbl.toString().length();
		String temp = htbl.toString().substring(1, length - 1);
		String [] tempArray = temp.split(", ");
		ArrayList<String> values = new ArrayList<String>();
		for(int i = 0; i < tempArray.length; i++) {
			String [] tempArray2 = tempArray[i].split("=");
			values.add(tempArray2[1]);
		}
		return values;
	}
	
	public void createIndex(String strTableName,
			String strColName)
			throws DBAppException{
		if(tables.containsKey(strTableName)) {
			BTree root = new BTree();
			tables.get(strTableName).addToIndices(strColName,root);
			tables.get(strTableName).getIndexedCols().add(strColName);
			String file = strTableName + strColName + ".class";
			try {
				fileOut = new FileOutputStream(file); // Create a page for indices
			} catch(IOException e) {
				
			}
		} else {
			throw new DBAppException("No Table Exists With That Name");
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
			temproot.insert((String)htblColNameValue.get(key), "" + strTableName + pCount); // Insert into BTree. Key = Column's value. Value = tableName + pageCount (Reference)
			String file = strTableName + key + ".class";
			try {
				fileOut = new FileOutputStream(file, true); // set output stream to index file
				out = new ObjectOutputStream(fileOut);
				String record = htblColNameValue.get(key) + "," + strTableName + pCount;
				out.writeObject(record); // insert index (key,value) into index file for whenever we want to recreate indices
			} catch(IOException e) {
				
			}
			
			
		}
	}
	
	public void deleteFromTable(String strTableName,
			Hashtable<String,String> htblColNameValue,
			String strOperator)
			throws DBEngineException{
		if(strOperator.toLowerCase().equals("or")) {
			deleteWhenOr(strTableName, htblColNameValue);
		} else {
			deleteWhenAdd(strTableName, htblColNameValue);
		}
		
			
	}
	
	public void deleteWhenAdd(String strTableName,
			Hashtable<String,String> htblColNameValue) {
		ArrayList<String> colNames = htblKeys(htblColNameValue); // Column Names of records to be deleted
		ArrayList<String> colValues = htblValues(htblColNameValue); // Corresponding values of columns
		Table table = tables.get(strTableName);
		Hashtable<String,BTree> indices = table.getIndices(); // Get the hash table containing the indexed columns and their BTrees
		ArrayList<String> indexedCols = table.getIndexedCols();
		String col = "";
		for(int i = 0; i < colNames.size(); i++) { // Searching if any column is indexed
			col = colNames.get(i);
			if(indexedCols.contains(col)) {
				break;
			}
		}
		if(!col.equals("")) { // If there's an indexed column, then there will be no duplicates
			BTree root = indices.get(col);
			String val = htblColNameValue.get(col);
			String ref = (String)root.search(val);
			if(!ref.equals(null)) {
				ArrayList<Hashtable<String,String>> records = table.getHandler().loadRecordsFromPage(ref);
				for(int i = 0; i < records.size(); i++) {
					Hashtable<String,String> current = records.get(i); // Current record in page
					if(current.get(col).equals(val)) { // If the indexed column's value equals the current record's value
						/* Checking that all the conditions' values are equals to current record's values 
						 * Either there is a record to delete, otherwise there will be no record at all because 
						 * one of the columns are indexed (No duplicates)*/
						for(int j = 0; j < colNames.size(); j++) { 
							if(!current.get(colNames.get(j)).equals(colValues.get(j))) {
								return; // Return if one of the conditions is not true
							}
						}
						records.remove(i);
						root.delete(col);
						break;
					}
				}
				table.getHandler().deleteFile(ref); // Delete the file where the record existed
				table.getHandler().writeRecordsToPage(ref, records); // Recreate the file without the deleted record
			} else {
				return; // The indexed value was not found, therefore there is no record
			}
		} else { // No indexed columns
			int pCount = table.getHandler().getpCount();
			for(int i = 0; i <= pCount; i++) {
				String ref = strTableName + i + ".class";
				ArrayList<Hashtable<String,String>> records = table.getHandler().loadRecordsFromPage(ref);
				for(int j = 0; j < records.size(); j++) { // Loop through each single page
					Hashtable<String,String> current = records.get(j);
					boolean delete = true;
					/* Checks on each record whether it meets all the conditions, if not get next record and keep checking*/
					for(int k = 0; k < colNames.size(); k++) { 
						if(!(current.get(colNames.get(k)).equals(colValues.get(k)))) { // One of the conditions are not true
							delete = false;
							break;
						}
					}
					if(delete) { // All conditions are true
						records.remove(j);
					}
				}
				table.getHandler().deleteFile(ref); // Delete the file where the record existed
				table.getHandler().writeRecordsToPage(ref, records); // Recreate the file without the deleted record
			}
		}
		
	}
	
	public void deleteWhenOr(String strTableName,
			Hashtable<String,String> htblColNameValue) {
		ArrayList<String> colNames = htblKeys(htblColNameValue); // Column Names of records to be deleted
		ArrayList<String> colValues = htblValues(htblColNameValue); // Corresponding values of columns
		Table table = tables.get(strTableName);
		Hashtable<String,BTree> indices = table.getIndices(); // Get the hash table containing the indexed columns and their BTrees
		for(int i = 0; i < colNames.size(); i++) {
			String col = colNames.get(i);
			String val = colValues.get(i);
			if(table.getIndexedCols().contains(col)) { // Check if a column is indexed
				BTree root = indices.get(col);
				String ref = (String)root.search(htblColNameValue.get(col)); // Get record's reference/page
				if(!ref.equals(null)){ // If the record exists
					ArrayList<Hashtable<String,String>> records = table.getHandler().loadRecordsFromPage(ref);
					for(int j = 0; j < records.size(); j++) {
						Hashtable<String,String> current = records.get(j);
						if(current.get(col).equals(val)) { // Remove record from the records array list
							records.remove(j);
							root.delete(col);
							break;
						}
					}
					table.getHandler().deleteFile(ref); // Delete the file where the record existed
					table.getHandler().writeRecordsToPage(ref, records); // Recreate the file without the deleted record
				} 
			} else { // If the columns is not indexed
				int pCount = table.getHandler().getpCount();
				for(int j = 0; j <= pCount; j++) { // Loop through all pages
					String ref = strTableName + j + ".class";
					ArrayList<Hashtable<String,String>> records = table.getHandler().loadRecordsFromPage(ref);
					for(int k = 0; k < records.size(); k++) { // Loop through each single page
						Hashtable<String,String> current = records.get(k);
						if(current.get(col).equals(val)) { // Remove record from the records array list
							records.remove(j);
							break; // HANDLE DUPLICATES?
						}
					}
					table.getHandler().deleteFile(ref); // Delete the file where the record existed
					table.getHandler().writeRecordsToPage(ref, records); // Recreate the file without the deleted record
				}
			}
		}
	}
	
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
		System.out.println(balance.toString().substring(1, balance.toString().length()-1));
		System.out.println(a.htblKeys(balance));
		System.out.println(a.htblValues(balance));
		names = balance.keys();
		String []b = null;
		Set<String> indexedColNames = balance.keySet();
		ArrayList<String> records = new ArrayList<String>();
		records.add("Lol");
		records.add("Lol1");
		records.add("Lol2");
		records.add("Lol3");
		records.remove("Lol2");
		System.out.println(records.toString());
		System.out.println("" + records.contains("Lol2") + records.contains("Lol3"));
		
		System.out.println(balance.keySet().iterator().next());

		System.out.println(balance.keySet().remove(balance.keySet().iterator().next()));
		  
		System.out.println(balance.keySet().iterator().next());
	      
	}

}
