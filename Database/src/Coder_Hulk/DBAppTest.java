package Coder_Hulk;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

public class DBAppTest {
	DBApp test;
	String tableName;
	Hashtable<String,String> htblColNameType;
	Hashtable<String,String> htblColNameRefs;
	String key;
	public DBAppTest() {
		test = new DBApp();
		tableName = "Employee";
		htblColNameType = new Hashtable<String,String>();
		htblColNameType.put("ID", "Java.Integer");
		htblColNameType.put("Name", "Java.String");
		htblColNameType.put("Age", "Java.Integer");
		htblColNameType.put("Salary", "Java.Double");
		htblColNameType.put("Dept", "Java.Integer");
		htblColNameRefs = new Hashtable<String,String>();
		htblColNameRefs.put("Dept", "Department.ID");
		key = "ID";
	}
	
	/* Create Table WORKS TODO Assertion !!!*/
	public void testCreateTable() throws DBAppException{
		
		test.createTable(tableName, htblColNameType, htblColNameRefs, key);
		try {
			test.read = new BufferedReader(new FileReader ("metadata.csv"));
			String line = "";
	        while ((line = test.read.readLine()) != null) {
	        	System.out.println(line);
	        }
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			
		}
        
	}
	
	/* Create Index WORKS !!*/
	public void testCreateIndex() throws DBAppException {
		String colName = "Name";
		test.createTable(tableName, htblColNameType, htblColNameRefs, key);
		test.createIndex(tableName, colName);
		File file = new File(tableName + colName + ".class");
		if(file.exists() && test.tables.get(tableName).getIndexedCols().contains(colName) 
				&& test.tables.get(tableName).getIndices().containsKey(colName)) {
			System.out.println("Create Index works");
		}
		
	}
	
	public static void main(String[]args) {
		DBAppTest test = new DBAppTest();
		test.test.init();
		try {
			test.testCreateTable();
		} catch (DBAppException e) {
			e.printStackTrace();
		}

		try {
			test.testCreateIndex();
		} catch (DBAppException e) {
			e.printStackTrace();
		}
		
		
	}
}
