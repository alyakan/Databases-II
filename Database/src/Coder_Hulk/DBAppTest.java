package Coder_Hulk;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
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
		ArrayList<Hashtable<String,String>> records = new ArrayList<Hashtable<String,String>>();
		Hashtable<String,String> htblColNameValue = new Hashtable<String,String>();
		htblColNameValue.put("ID","2");
		htblColNameValue.put("Name","Aly");
		htblColNameValue.put("Age","2");
		htblColNameValue.put("Salary","2");
		htblColNameValue.put("Dept","2");
		records.add(htblColNameValue);
		
		try {
			test.test.insertIntoTable("Employee", htblColNameValue);
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		htblColNameValue = new Hashtable<String,String>();
		htblColNameValue.put("ID","3");
		htblColNameValue.put("Name","Rana");
		htblColNameValue.put("Age","2");
		htblColNameValue.put("Salary","2");
		htblColNameValue.put("Dept","2");
		try {
			test.test.insertIntoTable("Employee", htblColNameValue);
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		records.add(htblColNameValue);
		System.out.println(records.toString());
		test.test.tables.get("Employee").getHandler().getListOfPages().add(records);
		try {
			FileOutputStream fileOut = new FileOutputStream("test.class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(records);
			out.reset();
			out.close();
		} catch(IOException e) {
			
		}
		try {
			FileInputStream fileIn = new FileInputStream("test.class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			records = (ArrayList<Hashtable<String,String>>)in.readObject();
			System.out.println(records.toString());
			in.close();
		} catch(IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		
//		try {
//			Hashtable<String,String> delete = new Hashtable<String,String>();
//			delete.put("Name","Aly");
//			test.test.deleteFromTable("Employee", delete, "OR");
//		} catch (DBEngineException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
	}
}
