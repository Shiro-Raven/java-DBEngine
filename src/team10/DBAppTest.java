package team10;

import java.util.Date;
import java.util.Hashtable;

public class DBAppTest {
	public static void main(String[] args) throws Exception {
		// set the MaximumRowsCountinPage = 5 in DBApp.properties 
		// run the Tests class first to create the table
		
		Page p1 = PageManager.deserializePage("data/Student/page_1.ser");
		Page p2 = PageManager.deserializePage("data/Student/page_2.ser");
		Page p3 = PageManager.deserializePage("data/Student/page_3.ser");
		System.out.println("Table before deletion:\n");
		System.out.println("Page 1");
		PageManager.printPageContents(p1);
		System.out.println("Page 2");
		PageManager.printPageContents(p2);
		System.out.println("Page 3");
		PageManager.printPageContents(p3);
		
		Hashtable<String, Object> tuple = new Hashtable<>();
		
		// run as is and you'll get an exception
		
		// uncomment following line to test deleting a row based only on the primary key
		// tuple.put("id", 100);
		
		// uncomment the following lines to test deleting a row given all columns
		/*tuple.put("id", 50);
		tuple.put("first_name", "Hana");
		tuple.put("last_name", "Ismail");
		tuple.put("gpa", 0.92);
		tuple.put("birth_date", new Date(1996, 11, 26));
		tuple.put("gender", true);*/
		
		// uncomment following line to test deleting a row based on a non key column
		// tuple.put("gender", false);
		
		// uncomment following line to test deleting a row based on a non key column
		// tuple.put("first_name", "Yasmeen");
		
		// uncomment following line to test deleting a row based on a set of non key columns
		/*tuple.put("gender", true);
		tuple.put("gpa", 0.92);*/
		
		new DBApp().deleteFromTable("Student", tuple);
		
		System.out.println("\nTable after deletion:\n");
		p1 = PageManager.deserializePage("data/Student/page_1.ser");
		p2 = PageManager.deserializePage("data/Student/page_2.ser");
		p3 = PageManager.deserializePage("data/Student/page_3.ser");
		System.out.println("Page 1");
		PageManager.printPageContents(p1);
		System.out.println("Page 2");
		PageManager.printPageContents(p2);
		System.out.println("Page 3");
		PageManager.printPageContents(p3);
	}
}
