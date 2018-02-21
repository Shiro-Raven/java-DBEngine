package team10;

import java.util.Date;
import java.util.Hashtable;

public class DBAppTest {
	public static void main(String[] args) throws Exception {
		// run the Tests class first to create the table
		
		Page p = PageManager.deserializePage("data/Student/page_1.ser");
		System.out.println("Table before deletion:");
		PageManager.printPageContents(p);
		
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
		
		// uncomment following line to test deleting a row based on a set of non key columns
		/*tuple.put("gender", true);
		tuple.put("gpa", 0.92);*/
		
		new DBApp().deleteFromTable("Student", tuple);
		
		p = PageManager.deserializePage("data/Student/page_1.ser");
		System.out.println("Table after deletion:");
		PageManager.printPageContents(p);
	}
}
