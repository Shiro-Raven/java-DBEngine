package placeholder;

public class DBAppException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DBAppException()
	{
		super();
	}
	
	public DBAppException(String e)
	{
		System.out.println("Error! "+ e);
	}
}
