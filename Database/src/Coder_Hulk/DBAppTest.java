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
import java.util.concurrent.TimeUnit;

public class DBAppTest {
	DBApp test;
	String strTableName;
	Hashtable<String,String> htblColNameType;
	Hashtable<String,String> htblColNameRefs;
	String key;
	Hashtable<String,String> htblColNameValue;
	public DBAppTest() {
		test = new DBApp();
		strTableName = "Employee";
		htblColNameType = new Hashtable<String,String>();
		htblColNameType.put("ID", "Java.Integer");
		htblColNameType.put("Name", "Java.String");
		htblColNameType.put("Age", "Java.Integer");
		htblColNameType.put("Salary", "Java.Double");
		htblColNameType.put("Dept", "Java.Integer");
		htblColNameRefs = new Hashtable<String,String>();
		htblColNameRefs.put("Dept", "Department.ID");
		key = "ID";
		htblColNameValue = new Hashtable<String,String>();
		htblColNameValue.put("ID", "3");
		htblColNameValue.put("Name", "Rana");
		htblColNameValue.put("Age", "20");
		htblColNameValue.put("Dept", "CS");
	}
	
	/* Create Table WORKS TODO Assertion !!!*/
	public void testCreateTable() throws DBAppException{
		
		test.createTable(strTableName, htblColNameType, htblColNameRefs, key);
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
	public void testCreateIndex() throws DBAppException, IOException {
		String colName = "Name";
		test.createTable(strTableName, htblColNameType, htblColNameRefs, key);
		test.createIndex(strTableName, colName);
		File file = new File(strTableName + colName + ".class");
		if(file.exists() && test.tables.get(strTableName).getIndexedCols().contains(colName) 
				&& test.tables.get(strTableName).getIndices().containsKey(colName)) {
			System.out.println("Create Index works");
		}
		
	}
	
	public void testInsertIntoTable(String strTableName,
			Hashtable<String,String> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {
		test.insertIntoTable(strTableName, htblColNameValue);
		test.insertIntoTable(strTableName, htblColNameValue);
		System.out.println(test.tables.get(strTableName).getPages().getpagesList().get(0).toString());
		test.tables.get(strTableName).savePages();
		Pages p = test.tables.get(strTableName).loadPages();
		System.out.println(p.getpagesList().get(0).toString());
		
		test.tables.get(strTableName).saveIndices();
		Hashtable<String, ArrayList<String>> t = test.tables.get(strTableName).loadIndices();
		System.out.println(t.get("ID").toString());
	
		
	}
	
	public void testDeleteFromTable(String strTableName,
			Hashtable<String,String> htblColNameValue,
			String strOperator)
			throws DBEngineException, IOException, ClassNotFoundException{
		
		test.deleteFromTable(strTableName, htblColNameValue, strOperator);

		//test.tables.get(strTableName).updatePages();
		Pages p = test.tables.get(strTableName).loadPages();
		System.out.println(p.getpagesList().get(0).toString());
		Hashtable<String, ArrayList<String>> t = test.tables.get(strTableName).loadIndices();
		System.out.println(t.get("ID").toString());


		
	}
	
	public static void main(String[]args) throws FileNotFoundException, IOException, ClassNotFoundException {
		DBAppTest test = new DBAppTest();
		test.test.init();
		try {
			test.testCreateTable();
		} catch (DBAppException e) {
			e.printStackTrace();
		}

//		try {
//			//test.testCreateIndex();
//		} //catch (DBAppException e) {
//			e.printStackTrace();
//		}
		
		try {
			test.testInsertIntoTable(test.strTableName, test.htblColNameValue);
			//System.out.println(test.test.tables.get(test.strTableName).getHandler().loadRecordsFromPage2("Employee0").toString());
		} catch (DBAppException e) {
			e.printStackTrace();
		}
		
		try {
			Hashtable<String,String> htblColNameDelete = new Hashtable<String,String>();
			htblColNameDelete.put("ID", "3");
			//htblColNameDelete.put("Name", "Aly");
			htblColNameDelete.put("Dept", "CS");

			//test.test.tables.get("Employee").updatePages();
		    test.testDeleteFromTable(test.strTableName, htblColNameDelete, "OR");
		   
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

//        ArrayList<ArrayList<Hashtable<String,String>>> listOfPages = new ArrayList<ArrayList<Hashtable<String,String>>>();
//		ArrayList<Hashtable<String,String>> A0 = new ArrayList<Hashtable<String,String>>();
//		Hashtable<String, String> htblColNameValue1 = new Hashtable<String,String>();
//		htblColNameValue1.put("ID", "3");
//		htblColNameValue1.put("Name", "Rana");
//		htblColNameValue1.put("Age", "20");
//		htblColNameValue1.put("Dept", "CS");
//		A0.add(htblColNameValue1);
//		A0.add(htblColNameValue1);
//		listOfPages.add(A0);
//
//		//System.out.println(listOfPages);
//
//        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File("t.class")));
//        oos.writeObject(listOfPages);
//        oos.close();
//
//
//        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File("t.class")));
//        ArrayList<ArrayList<Hashtable<String,String>>> t = (ArrayList<ArrayList<Hashtable<String,String>>>)ois.readObject();
//        ois.close();
//
//        System.out.println(t.get(0).get(0));

		
		
	}
}
