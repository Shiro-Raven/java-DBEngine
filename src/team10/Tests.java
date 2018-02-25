package team10;

import java.util.Date;
import java.util.Hashtable;

public class Tests {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
//
//		// Declaring Columns & Their Types
//		Hashtable<String, String> htblColNameType = new Hashtable<>();
//		htblColNameType.put("id", Integer.class.getName());
//		htblColNameType.put("first_name", String.class.getName());
//		htblColNameType.put("last_name", String.class.getName());
//		htblColNameType.put("gpa", Double.class.getName());
//		htblColNameType.put("birthdate", Date.class.getName());
//		htblColNameType.put("gender", Boolean.class.getName());
//		// false = Male / true = Female
//		// Women are always right, right?
//
//		// Making The Table With "id" As The Clustering Key
//		new DBApp().createTable("Student", "id", htblColNameType);
//
//		// Insertions
//		// 1
//		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
//		htblColNameValue.put("id", 1);
//		htblColNameValue.put("first_name", "Ahmed");
//		htblColNameValue.put("last_name", "Mohammed");
//		htblColNameValue.put("gpa", 1.91);
//		htblColNameValue.put("birth_date", new Date(1997, 5, 5));
//		htblColNameValue.put("gender", false);
//		new DBApp().insertIntoTable("Student", htblColNameValue);
//
//		// 2
//		htblColNameValue.put("id", 2);
//		htblColNameValue.put("first_name", "Sarah");
//		htblColNameValue.put("last_name", "Khaled");
//		htblColNameValue.put("gpa", 0.92);
//		htblColNameValue.put("birth_date", new Date(1997, 4, 9));
//		htblColNameValue.put("gender", true);
//		new DBApp().insertIntoTable("Student", htblColNameValue);
//
//		// 3
//		htblColNameValue.put("id", 50);
//		htblColNameValue.put("first_name", "Hana");
//		htblColNameValue.put("last_name", "Ismail");
//		htblColNameValue.put("gpa", 0.92);
//		htblColNameValue.put("birth_date", new Date(1996, 11, 26));
//		htblColNameValue.put("gender", true);
//		new DBApp().insertIntoTable("Student", htblColNameValue);
//
//		// 4
//		htblColNameValue.put("id", 100);
//		htblColNameValue.put("first_name", "Omar");
//		htblColNameValue.put("last_name", "Elsayed");
//		htblColNameValue.put("gpa", 1.56);
//		htblColNameValue.put("birth_date", new Date(1997, 7, 12));
//		htblColNameValue.put("gender", false);
//		new DBApp().insertIntoTable("Student", htblColNameValue);
//
//		// 5
//		htblColNameValue.put("id", 197);
//		htblColNameValue.put("first_name", "Mostafa");
//		htblColNameValue.put("last_name", "Hashem");
//		htblColNameValue.put("gpa", 2.16);
//		htblColNameValue.put("birth_date", new Date(1997, 2, 17));
//		htblColNameValue.put("gender", false);
//		new DBApp().insertIntoTable("Student", htblColNameValue);
//
//		// 6
//		htblColNameValue.put("id", 105);
//		htblColNameValue.put("first_name", "Yasmeen");
//		htblColNameValue.put("last_name", "Khalafy");
//		htblColNameValue.put("gpa", 0.92);
//		htblColNameValue.put("birth_date", new Date(1997, 1, 1));
//		htblColNameValue.put("gender", true);
//		new DBApp().insertIntoTable("Student", htblColNameValue);
//
//		// 7
//		htblColNameValue.put("id", 106);
//		htblColNameValue.put("first_name", "Yasmeen");
//		htblColNameValue.put("last_name", "Khalafy");
//		htblColNameValue.put("gpa", 0.92);
//		htblColNameValue.put("birth_date", new Date(1997, 1, 1));
//		htblColNameValue.put("gender", true);
//		new DBApp().insertIntoTable("Student", htblColNameValue);
//		
//		System.out.println("Done");
//		
//		
		// "Student"
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put("id", 107);
//		htblColNameValue.put("first_name", "Hamada");
//		htblColNameValue.put("last_name", "Yasta");
//		htblColNameValue.put("gpa", 2.0);
//		htblColNameValue.put("birth_date", new Date(1900, 1, 1));
//		htblColNameValue.put("gender", false);
//		new DBApp().insertIntoTable("Student", htblColNameValue);
//		new DBApp().updateTable("Student", "197", htblColNameValue);
//		System.out.println(UpdateUtilities.checkNotUsed("Student", 197, "id"));
		PageManager.printPageContents(PageManager.deserializePage("data/Student/page_1.ser"));

	}

}