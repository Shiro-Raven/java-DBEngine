package team10;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;

public class Tests {
	// redo this test in this order and discover new bugs
	public static void main(String[] args) throws DBAppException, Exception, IOException {
		int[] id = { 5, 4, 1, 2, 3, 6, 7, 10, 9, 8, 54, 53, 244, 21, 56 };
		// failed! problem: neighboring duplicates, and perhaps more...
		 String[] firstName = { "ahmed","ahmed","hell","hell",
		 "mohamed","zzz","lovelife","psychopass new movie","yes, darwish, rejoice",
		 "reda is a computer fanatic... or is he?","i am evil","i am zen", "i am aku",
		 "life is a waterfall",""};

		 //passed!
//		 String[] firstName = { "ahmed","notAhmed","hell","nothell",
//		 "mohamed","zzz","lovelife","psychopass new movie","yes Darwish rejoice",
//		 "reda is a computer fanatic or is he?","i am evil","ahmed", "i am aku",
//		 "life is a waterfall",""};
		for (int i = 0; i < id.length; i++) {
			Hashtable<String, Object> row = new Hashtable<>();
			row.put("id", id[i]);
			row.put("first_name", firstName[i]);
			DBApp app = new DBApp();
			app.insertIntoTable("idTest", row);
		}

		// Hashtable<String, Object> row = new Hashtable<>();
		// row.put("id", 1);
		// row.put("first_name", "hell");
		// DBApp app = new DBApp();
		// app.insertIntoTable("idTest", row);

		System.out.println("Table:");
		for (int i = 1; i <= 3; i++) {
			Page page = PageManager.deserializePage("data/idTest/page_" + i + ".ser");
			PageManager.printPageContents(page);
		}
		System.out.println("Dense Index on First Name:");
		for (int i = 1; i <= 3; i++) {
			Page page = PageManager.deserializePage("data/idTest/first_name/Indices/Dense/page_" + i + ".ser");
			PageManager.printPageContents(page);
		}

	}

	static void index() throws Exception {
		Page p1 = new Page(1, PageType.BRIN);
		Page p2 = new Page(2, PageType.BRIN);
		Hashtable<String, Object> row1 = new Hashtable<>();
		row1.put("idMax", 202);
		row1.put("idMin", 34);
		row1.put("pageNumber", 1);

		Hashtable<String, Object> row2 = new Hashtable<>();
		row2.put("idMax", 383);
		row2.put("idMin", 217);
		row2.put("pageNumber", 2);

		Hashtable<String, Object> row3 = new Hashtable<>();
		row3.put("idMax", 590);
		row3.put("idMin", 438);
		row3.put("pageNumber", 3);

		Hashtable<String, Object> row4 = new Hashtable<>();
		row4.put("idMax", 984);
		row4.put("idMin", 671);
		row4.put("pageNumber", 4);

		p1.getRows()[0] = row1;
		p1.getRows()[1] = row2;

		p2.getRows()[0] = row3;
		p2.getRows()[1] = row4;

		PageManager.serializePage(p1, "data/idTest/id/page_1.ser");
		PageManager.serializePage(p2, "data/idTest/id/page_2.ser");

	}

	static void temp() throws Exception {
		Hashtable<String, String> htblColNameType = new Hashtable<>();
		htblColNameType.put("id", Integer.class.getName());
		htblColNameType.put("first_name", String.class.getName());
		htblColNameType.put("last_name", String.class.getName());

		DBApp app = new DBApp();
		ArrayList<Integer> ids = new ArrayList<>();
		app.createTable("idTest", "id", htblColNameType);

		for (int i = 0; i < 20; i++) {
			int id = (int) (Math.random() * 1000);
			Hashtable<String, Object> row = new Hashtable<>();
			row.put("id", id);
			ids.add(id);
			try {
				app.insertIntoTable("idTest", row);
			} catch (Exception e) {
				i--;
				continue;
			}
		}
		Collections.sort(ids);
		for (int i = 0; i < ids.size(); i++) {
			System.out.print(ids.get(i) + ", ");
		}

		for (int i = 1; i <= 4; i++) {
			Page page = PageManager.deserializePage("data/idTest/page_" + i + ".ser");
			PageManager.printPageContents(page);
		}
	}

	// @SuppressWarnings("deprecation")
	// public static void main(String[] args) throws Exception {
	//
	// Declaring Columns & Their Types
	// @SuppressWarnings("unused")
	// Hashtable<String, String> htblColNameType = new Hashtable<>();
	// htblColNameType.put("id", Integer.class.getName());
	// htblColNameType.put("first_name", String.class.getName());
	// htblColNameType.put("last_name", String.class.getName());
	// htblColNameType.put("gpa", Double.class.getName());
	// htblColNameType.put("birthdate", Date.class.getName());
	// htblColNameType.put("gender", Boolean.class.getName());
	// // false = Male / true = Female
	// // Women are always right, right?
	//
	// // Making The Table With "id" As The Clustering Key
	// new DBApp().createTable("Student", "id", htblColNameType);
	//
	// // Insertions
	// // 1
	// Hashtable<String, Object> htblColNameValue = new Hashtable<>();
	// htblColNameValue.put("id", 1);
	// htblColNameValue.put("first_name", "Ahmed");
	// htblColNameValue.put("last_name", "Mohammed");
	// htblColNameValue.put("gpa", 1.91);
	// htblColNameValue.put("birth_date", new Date(1997, 5, 5));
	// htblColNameValue.put("gender", false);
	// new DBApp().insertIntoTable("Student", htblColNameValue);
	//
	// // 2
	// htblColNameValue.put("id", 2);
	// htblColNameValue.put("first_name", "Sarah");
	// htblColNameValue.put("last_name", "Khaled");
	// htblColNameValue.put("gpa", 0.92);
	// htblColNameValue.put("birth_date", new Date(1997, 4, 9));
	// htblColNameValue.put("gender", true);
	// new DBApp().insertIntoTable("Student", htblColNameValue);
	//
	// // 3
	// htblColNameValue.put("id", 50);
	// htblColNameValue.put("first_name", "Hana");
	// htblColNameValue.put("last_name", "Ismail");
	// htblColNameValue.put("gpa", 0.92);
	// htblColNameValue.put("birth_date", new Date(1996, 11, 26));
	// htblColNameValue.put("gender", true);
	// new DBApp().insertIntoTable("Student", htblColNameValue);
	//
	// // 4
	// htblColNameValue.put("id", 100);
	// htblColNameValue.put("first_name", "Omar");
	// htblColNameValue.put("last_name", "Elsayed");
	// htblColNameValue.put("gpa", 1.56);
	// htblColNameValue.put("birth_date", new Date(1997, 7, 12));
	// htblColNameValue.put("gender", false);
	// new DBApp().insertIntoTable("Student", htblColNameValue);
	//
	// // 5
	// htblColNameValue.put("id", 197);
	// htblColNameValue.put("first_name", "Mostafa");
	// htblColNameValue.put("last_name", "Hashem");
	// htblColNameValue.put("gpa", 2.16);
	// htblColNameValue.put("birth_date", new Date(1997, 2, 17));
	// htblColNameValue.put("gender", false);
	// new DBApp().insertIntoTable("Student", htblColNameValue);
	//
	// // 6
	// htblColNameValue.put("id", 105);
	// htblColNameValue.put("first_name", "Yasmeen");
	// htblColNameValue.put("last_name", "Khalafy");
	// htblColNameValue.put("gpa", 0.92);
	// htblColNameValue.put("birth_date", new Date(1997, 1, 1));
	// htblColNameValue.put("gender", true);
	// new DBApp().insertIntoTable("Student", htblColNameValue);
	//
	// // 7
	// htblColNameValue.put("id", 106);
	// htblColNameValue.put("first_name", "Yasmeen");
	// htblColNameValue.put("last_name", "Khalafy");
	// htblColNameValue.put("gpa", 0.92);
	// htblColNameValue.put("birth_date", new Date(1997, 1, 1));
	// htblColNameValue.put("gender", true);
	// new DBApp().insertIntoTable("Student", htblColNameValue);
	//
	// htblColNameValue.put("id", 46);
	// htblColNameValue.put("first_name", "Ahmed");
	// htblColNameValue.put("last_name", "Mohammed");
	// htblColNameValue.put("gpa", 1.91);
	// htblColNameValue.put("birth_date", new Date(1997, 5, 5));
	// htblColNameValue.put("gender", false);
	// new DBApp().insertIntoTable("Student", htblColNameValue);
	//
	// htblColNameValue.put("id", 68);
	// htblColNameValue.put("first_name", "Sarah");
	// htblColNameValue.put("last_name", "Khaled");
	// htblColNameValue.put("gpa", 0.92);
	// htblColNameValue.put("birth_date", new Date(1997, 4, 9));
	// htblColNameValue.put("gender", true);
	// new DBApp().insertIntoTable("Student", htblColNameValue);
	//
	// htblColNameValue.put("id", 51);
	// htblColNameValue.put("first_name", "Hana");
	// htblColNameValue.put("last_name", "Ismail");
	// htblColNameValue.put("gpa", 0.92);
	// htblColNameValue.put("birth_date", new Date(1996, 11, 26));
	// htblColNameValue.put("gender", true);
	// new DBApp().insertIntoTable("Student", htblColNameValue);
	//
	// htblColNameValue.put("id", 166);
	// htblColNameValue.put("first_name", "Omar");
	// htblColNameValue.put("last_name", "Elsayed");
	// htblColNameValue.put("gpa", 1.56);
	// htblColNameValue.put("birth_date", new Date(1997, 7, 12));
	// htblColNameValue.put("gender", false);
	// new DBApp().insertIntoTable("Student", htblColNameValue);
	//
	// htblColNameValue.put("id", 178);
	// htblColNameValue.put("first_name", "Mostafa");
	// htblColNameValue.put("last_name", "Hashem");
	// htblColNameValue.put("gpa", 2.16);
	// htblColNameValue.put("birth_date", new Date(1997, 2, 17));
	// htblColNameValue.put("gender", false);
	// new DBApp().insertIntoTable("Student", htblColNameValue);
	//
	// htblColNameValue.put("id", 501);
	// htblColNameValue.put("first_name", "Yasmeen");
	// htblColNameValue.put("last_name", "Khalafy");
	// htblColNameValue.put("gpa", 0.95);
	// htblColNameValue.put("birth_date", new Date(1997, 1, 1));
	// htblColNameValue.put("gender", true);
	// new DBApp().insertIntoTable("Student", htblColNameValue);

	// System.out.println("Done");
	//

	// "Student"
	// Tests for the update
	// htblColNameValue.put("id", 107);
	// htblColNameValue.put("first_name", 5);
	// htblColNameValue.put("last_name", null);
	// htblColNameValue.put("gpa", 2.0);
	// htblColNameValue.put("birth_date", new Date(1900, 1, 1));
	// htblColNameValue.put("gender", false);
	// new DBApp().updateTable("Student", "106", htblColNameValue);
	// PageManager.printPageContents(PageManager.deserializePage("data/Student/page_2.ser"));

	/*
	 * // Deletion Tests // set the MaximumRowsCountinPage = 5 in
	 * DBApp.properties
	 * 
	 * Page p1 = PageManager.deserializePage("data/Student/page_1.ser"); Page p2
	 * = PageManager.deserializePage("data/Student/page_2.ser"); Page p3 =
	 * PageManager.deserializePage("data/Student/page_3.ser");
	 * System.out.println("Table before deletion:\n");
	 * System.out.println("Page 1"); PageManager.printPageContents(p1);
	 * System.out.println("Page 2"); PageManager.printPageContents(p2);
	 * System.out.println("Page 3"); PageManager.printPageContents(p3);
	 * 
	 * Hashtable<String, Object> tuple = new Hashtable<>();
	 * 
	 * // run as is and you'll get an exception
	 * 
	 * // uncomment following line to test deleting a row based only on the
	 * primary key // tuple.put("id", 100);
	 * 
	 * // uncomment the following lines to test deleting a row given all columns
	 * tuple.put("id", 50); tuple.put("first_name", "Hana");
	 * tuple.put("last_name", "Ismail"); tuple.put("gpa", 0.92);
	 * tuple.put("birth_date", new Date(1996, 11, 26)); tuple.put("gender",
	 * true);
	 * 
	 * // uncomment following line to test deleting a row based on a non key
	 * column // tuple.put("gender", false);
	 * 
	 * // uncomment following line to test deleting a row based on a non key
	 * column // tuple.put("first_name", "Yasmeen");
	 * 
	 * // uncomment following line to test deleting a row based on a set of non
	 * key columns tuple.put("gender", true); tuple.put("gpa", 0.92);
	 * 
	 * new DBApp().deleteFromTable("Student", tuple);
	 * 
	 * System.out.println("\nTable after deletion:\n"); p1 =
	 * PageManager.deserializePage("data/Student/page_1.ser"); p2 =
	 * PageManager.deserializePage("data/Student/page_2.ser"); p3 =
	 * PageManager.deserializePage("data/Student/page_3.ser");
	 * System.out.println("Page 1"); PageManager.printPageContents(p1);
	 * System.out.println("Page 2"); PageManager.printPageContents(p2);
	 * System.out.println("Page 3"); PageManager.printPageContents(p3);
	 * 
	 * // End of Deletion Tests
	 */

	// }

}