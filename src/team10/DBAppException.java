package team10;

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
		super("Error!"+e);
		//System.out.println("Error! "+ e);
	}
}
