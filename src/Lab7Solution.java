import java.sql.*; 
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Lab7Solution {
	public static String databaseName = "SCC201Lab7"; // a common variable for the database name


	public static Connection CreateConnection()//Connection creation function.. Pass by reference!
	{
		String jdbcURL = "jdbc:mariadb://localhost:3306/";
		final String username = "root";
		final String password = "";
		
		Connection connection = null;
		try
		{ 
			connection = DriverManager.getConnection(jdbcURL, username, password);
		}
		catch (SQLException e) 
		{
			System.out.println("This is the error");
		    e.printStackTrace();
		}		
		return connection;
	}

	public static void myQueries()
    {
    
		
		// Establishing a connection to the MySQL server
		Connection connection = CreateConnection();
		// Creating a Statement object for executing SQL commands
		try
		{
			Statement statement = connection.createStatement();
		    String UseDatabase = "use " + databaseName;//Before querying I need to select the database...
		    statement.executeUpdate(UseDatabase);
		    
		    //Query A
		    String queryA = "SELECT G1.myTitle FROM Game G1, Metadata M1 WHERE G1.myTitle = M1.myTitle AND Metadata_Publisher="+"'\"Nintendo\"'";    
		    Statement qA = connection.createStatement();
		    ResultSet qARes = qA.executeQuery(queryA);//Result set is created to keep the table...
		    if(qARes.next()==false)
		    {
		    	System.out.println("Result for the first query is emty");
		    }
		    else//process the result set.
		    {
		    	String address = qARes.getString(1);
		    	System.out.println("Result for the first query  is:..");
		    	System.out.println(address);
		    	while(qARes.next())
		    	{
			    	address = qARes.getString(1);
			    	System.out.println(address);
		    	}
		    }
		    connection.close();//close the connection..
		}
		catch (SQLException e) 
		{
			e.printStackTrace();
		}
		finally {
		    if (connection != null) {
		        try {
		        	connection.close();
		        } 
		        catch (SQLException e) { /* Ignored */}
		    }
		}	
    }

    public static void populateGameTable(String [][] data, int c, int r)
    { 
		String sqlInsert = "INSERT IGNORE INTO Game(myTitle,Features_Max_Players) VALUES(?,?)"; //Prepared statement for inserting large tuples... 
		
		// Establishing a connection to the MySQL server
	    Connection connection = CreateConnection();
	    
	    try{
			 // Creating a Statement and a PreparedStatement object for executing SQL commands
		    Statement statement = connection.createStatement();
		    PreparedStatement pst = connection.prepareStatement(sqlInsert);	 
		    String UseDatabase = "use " + databaseName;//Select the database..
		    statement.executeUpdate(UseDatabase);
		    for(int i = 0 ; i < r-2 ; i++)// we have to subtract one from the number of rows due to 1 row spent for the headings!
		    {
		    	String myVal = data[2][i];
		    	char value = (myVal.charAt(1));
		       	pst.setString(1,data[0][i]);//title is located in the 0th column, 
		    	pst.setInt(2,Integer.parseInt(String.valueOf(value)));// Max players are located in the 2th column
		    	pst.executeUpdate();
	    	}
	    	connection.close();//close the connection
		}
		catch (SQLException e) 
		{
			e.printStackTrace();
		}
		finally 
		{
		    if (connection != null) 
		    {
		        try 
		        {
		        	connection.close();
		        } 
		        catch (SQLException e) { /* Ignored */}
		    }
		}		  
    }
    public static void populateMetadataTable(String [][] data, int c, int r)
    {
   
		String sqlInsert = "INSERT IGNORE INTO Metadata(myTitle, Metadata_Genres, Metadata_Publisher) VALUES(?,?,?)";
		 
		// Establishing a connection to the MySQL server
	    Connection connection = CreateConnection();
	    
		 try{
	    
			 // Creating a Statement and a PreparedStatement object for executing SQL commands
			Statement statement = connection.createStatement();
			PreparedStatement pst = connection.prepareStatement(sqlInsert);	 
			String UseDatabase = "use " + databaseName;//select the database..
			statement.executeUpdate(UseDatabase);
			for(int i = 0 ; i < r-2 ; i++)// we have to subtract one from number of rows due to headings!
			{
		    		pst.setString(1,data[0][i]);//title is located in the 0th column, 
				pst.setString(2,data[5][i]);//Metadata_Genres is located in the 5th column, 
		    	        pst.setString(3,data[7][i]);//Metadata_Publisher is located in the 7th column, 
		    	        
		    	
		    	pst.executeUpdate();
			}
			connection.close();
		}
		catch (SQLException e) 
		{
			e.printStackTrace();
		}
		finally 
		{
		    if (connection != null) 
		    {
		        try 
		        {
		        	connection.close();
		        }
		        catch (SQLException e) { /* Ignored */}
		    }
		}		    
    }

	public static void CreateTables()
	{	
		// Establishing a connection to the MySQL server
		Connection connection = CreateConnection();
		try{
		    // Creating a Statement object for executing SQL commands
		    Statement statement = connection.createStatement();
		    // Creating the database
		    String createDatabase = "CREATE DATABASE IF NOT EXISTS " + databaseName;
		    statement.executeUpdate(createDatabase);
		    String UseDatabase = "use " + databaseName;
		    statement.executeUpdate(UseDatabase);
		    System.out.println("Database '" + databaseName + "' created and selected successfully.");
		    
		    String createPalBasicTable = "CREATE TABLE IF NOT EXISTS Game(myTitle VARCHAR(70), Features_Max_Players INTEGER, PRIMARY KEY(myTitle))";
		    statement.executeUpdate(createPalBasicTable);
		    
		    String createPalStatsTable = "CREATE TABLE IF NOT EXISTS Metadata(myTitle VARCHAR(70), Metadata_Genres VARCHAR(70),Metadata_Publisher VARCHAR(70), PRIMARY KEY(myTitle, Metadata_Genres), FOREIGN KEY(myTitle) REFERENCES Game(myTitle) ON DELETE CASCADE)";
		    

		    statement.executeUpdate(createPalStatsTable);
		    System.out.println("PalBasic and PalStats are created and selected successfully.");
		    connection.close();//Close the connection
		} 
		catch (SQLException e) 
		{
		    e.printStackTrace();
		    
		}
		finally 
		{
		    if (connection != null) 
		    {
		        try 
		        {
		        	connection.close();
		        }
		        catch (SQLException e) { /* Ignored */}
		    }
		}
	}

	public void reader(String fileName)
	{
        try (BufferedReader br = new BufferedReader(new FileReader(fileName)))
        {
            String line;
            List<String[]> allRows = new ArrayList<>();

            // Read each line from the CSV file
            while ((line = br.readLine()) != null) {
                // Split the line into an array of values using a comma as the delimiter
                String[] row = line.split(",");
                allRows.add(row);
            }

            // Assuming the first row contains column headers
            String[] headers = allRows.get(0);

            // Create arrays for each column dynamically
            int numColumns = headers.length;
            String[][] dataArrays = new String[numColumns][];

            // Initialize arrays
            for (int i = 0; i < numColumns; i++) {
                dataArrays[i] = new String[allRows.size()];
            }
            System.out.println(allRows.size());
            System.out.println(numColumns);

            // Populate arrays with data
            for (int i = 1; i < allRows.size()-1; i++) {
                String[] row = allRows.get(i);
                for (int j = 0; j < numColumns; j++) {
                    dataArrays[j][i - 1] = row[j];
                }
            }
            System.out.println("Reading "+fileName+" is completed... There are "+numColumns+" attributes in the file..");
            
            // Do not need to print the contents for Lab 6.
            /*for (int i = 0; i < numColumns; i++) 
            {
                System.out.println("Attribute " + headers[i] + ": " + String.join(", ", dataArrays[i]));
            }*/            
            populateGameTable(dataArrays,numColumns,allRows.size());
            populateMetadataTable(dataArrays,numColumns,allRows.size());
            
           
        } 
        catch (IOException e) 
        {
            e.printStackTrace();
        }
	}	
	
	public static void main(String[] args) {
    	CreateTables();
        // Replace "your_file.csv" with the path to your CSV file
        String gamesCSV = "video_game.csv";
        
        
        Lab7Solution urazsReader = new Lab7Solution();
        System.out.println("Reading the "+ gamesCSV +" now");
        urazsReader.reader(gamesCSV);
       
        myQueries();
    }
}
