import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;


public class code {
    public static void main(String[] args) throws SQLException {
        new JiaJunDatabase();
        new MiguelDatabase();
    }
}

class JiaJunDatabase {
    String jdbcURL = "jdbc:mariadb://localhost:3306/";
    String databaseName = "SCC201_Coursework";

    public String[][] readCSVFile(String fileName) {
        List<String[]> allRows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            // Read each line from the CSV file
            while ((line = br.readLine()) != null) {
                // Split the line into an array of values using a comma as the delimiter
                String[] row = line.split(",");
                allRows.add(row);
            }
        } catch (IOException e) {
            e.printStackTrace();
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

        // Populate arrays with data
        for (int i = 1; i < allRows.size(); i++) {
            String[] row = allRows.get(i);
            for (int j = 0; j < numColumns; j++) {
                dataArrays[j][i - 1] = row[j];
            }
        }
        System.out.println(
                "Reading " + fileName + " is completed... There are " + numColumns + " attributes in the file..");

        // Example: Printing the data from the arrays
        /*
         * for (int i = 0; i < numColumns; i++) {
         * System.out.println("Attribute " + headers[i] + ": " + String.join(", ",
         * dataArrays[i]));
         * }
         */
        return dataArrays;
    }

    public void createTable() {
        try (Connection connection = DriverManager.getConnection(jdbcURL);
                Statement statement = connection.createStatement();) {

            statement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + databaseName);            
            String UseDatabase = "use " + databaseName;
		    statement.executeUpdate(UseDatabase);

            statement.executeUpdate("DROP TABLE IF EXISTS ALBUM_SONGS");
            statement.executeUpdate("DROP TABLE IF EXISTS ALBUM");
            statement.executeUpdate("DROP TABLE IF EXISTS SONG");
            statement.executeUpdate("DROP TABLE IF EXISTS CONTRACT");
            statement.executeUpdate("DROP TABLE IF EXISTS ARTIST");
            statement.executeUpdate("DROP TABLE IF EXISTS RECORD_LABEL");

            String createArtist = "CREATE TABLE IF NOT EXISTS ARTIST (" +
                "ARTIST_ID VARCHAR(50) NOT NULL, " +
                "ARTIST_NAME VARCHAR(50), " +
                "ARTIST_DOB DATE, " + 
                "ARTIST_DEBUT_DATE DATE, " +
                "PRIMARY KEY (ARTIST_ID))";

            String createSong = "CREATE TABLE IF NOT EXISTS SONG (" +
                "SONG_ID INT NOT NULL AUTO_INCREMENT, " +
                "ARTIST_ID VARCHAR(50) NOT NULL, " +
                "SONG_NAME VARCHAR(50), " +
                "SONG_LENGTH TIME, " +
                "PRIMARY KEY (SONG_ID, ARTIST_ID), " +
                "FOREIGN KEY (ARTIST_ID) REFERENCES ARTIST(ARTIST_ID) ON DELETE RESTRICT)";

            String createAlbum = "CREATE TABLE IF NOT EXISTS ALBUM (" +
                "ALBUM_ID INT NOT NULL, " +
                "ARTIST_ID VARCHAR(50) NOT NULL, " +
                "ALBUM_NAME VARCHAR(50), " +
                "PUBLISH_DATE DATE, " +
                "PRIMARY KEY (ALBUM_ID, ARTIST_ID), " +
                "FOREIGN KEY (ARTIST_ID) REFERENCES ARTIST(ARTIST_ID) ON DELETE RESTRICT)";
            
            String createAlbumSongs = "CREATE TABLE IF NOT EXISTS ALBUM_SONGS (" +
                "ALBUM_ID INT NOT NULL, " +
                "SONG_ID INT NOT NULL AUTO_INCREMENT, " +
                "PRIMARY KEY (ALBUM_ID, SONG_ID), " +
                "FOREIGN KEY (ALBUM_ID) REFERENCES ALBUM(ALBUM_ID) ON DELETE RESTRICT, " +
                "FOREIGN KEY (SONG_ID) REFERENCES SONG(SONG_ID) ON DELETE RESTRICT, " +
                "UNIQUE (ALBUM_ID, SONG_ID))";
            

            String createRecordLabel = "CREATE TABLE IF NOT EXISTS RECORD_LABEL (" +
                "RECORD_LABEL_ID VARCHAR(10) NOT NULL, " +
                "RECORD_LABEL_NAME VARCHAR(50), " +
                "PRIMARY KEY (RECORD_LABEL_ID))";
            
            String createContract = "CREATE TABLE IF NOT EXISTS CONTRACT (" +
                "RECORD_LABEL_ID VARCHAR(10), " +
                "ARTIST_ID VARCHAR(50), " +
                "PRIMARY KEY (RECORD_LABEL_ID, ARTIST_ID), " +
                "FOREIGN KEY (ARTIST_ID) REFERENCES ARTIST(ARTIST_ID) ON DELETE RESTRICT, " +
                "FOREIGN KEY (RECORD_LABEL_ID) REFERENCES RECORD_LABEL(RECORD_LABEL_ID) ON DELETE RESTRICT)";

            statement.executeUpdate(createArtist);
            statement.executeUpdate(createSong);
            statement.executeUpdate(createAlbum);
            statement.executeUpdate(createAlbumSongs);
            statement.executeUpdate(createRecordLabel);
            statement.executeUpdate(createContract);

            System.out.println("Tables created successfully!");
        } catch (

        SQLException e) {
            e.printStackTrace();
        }

    }

    public void populateTable(String[][] jazzData) {
        try (Connection connection = DriverManager.getConnection(jdbcURL)) {
            Statement statement = connection.createStatement();
            String UseDatabase = "use " + databaseName;
		    statement.executeUpdate(UseDatabase);
            int rowCount = jazzData[0].length - 1;

            String insertArtist = "INSERT IGNORE INTO ARTIST (ARTIST_ID, ARTIST_NAME, ARTIST_DOB, ARTIST_DEBUT_DATE) VALUES (?, ?, ?, ?)";
            try (PreparedStatement ps = connection.prepareStatement(insertArtist)) {
                for (int j = 0; j < rowCount; j++) {
                    ps.setString(1, jazzData[0][j]);  
                    ps.setString(2, jazzData[4][j]); 
                    ps.setDate(3, Date.valueOf(jazzData[5][j]));       
                    ps.setDate(4, Date.valueOf(jazzData[6][j]));               
                    ps.executeUpdate();
                }
            }

            String insertSong = "INSERT IGNORE INTO SONG (ARTIST_ID, SONG_NAME, SONG_LENGTH) VALUES (?, ?, ?)";
            try (PreparedStatement ps = connection.prepareStatement(insertSong)) {
                for (int j = 0; j < rowCount; j++) {
                    ps.setString(1, jazzData[0][j]);  
                    ps.setString(2, jazzData[1][j]); 
                    ps.setTime(3, Time.valueOf(jazzData[8][j]));       
                    ps.executeUpdate();
                }
            }
            
            String insertAlbum = "INSERT IGNORE INTO ALBUM (ALBUM_ID, ARTIST_ID, ALBUM_NAME, PUBLISH_DATE) VALUES (?, ?, ?, ?)";
            try (PreparedStatement ps = connection.prepareStatement(insertAlbum)) {
                for (int j = 0; j < rowCount; j++) {
                    ps.setInt(1, Integer.valueOf(jazzData[2][j]));  
                    ps.setString(2, jazzData[0][j]);  
                    ps.setString(3, jazzData[3][j]); 
                    ps.setDate(4, Date.valueOf(jazzData[7][j]));       
                    ps.executeUpdate();
                }
            }

            String insertAlbumSongs = "INSERT IGNORE INTO ALBUM_SONGS (ALBUM_ID) VALUES (?)";
            try (PreparedStatement ps = connection.prepareStatement(insertAlbumSongs)) {
                for (int j = 0; j < rowCount; j++) {
                    ps.setInt(1, Integer.valueOf(jazzData[2][j]));     
                    ps.executeUpdate();
                }
            }

            String insertRecordLabel = "INSERT IGNORE INTO RECORD_LABEL (RECORD_LABEL_ID, RECORD_LABEL_NAME) VALUES (?, ?)";
            try (PreparedStatement ps = connection.prepareStatement(insertRecordLabel)) {
                for (int j = 0; j < rowCount; j++) {
                    ps.setString(1, jazzData[9][j]);     
                    ps.setString(2, jazzData[10][j]);     
                    ps.executeUpdate();
                }
            }
       
            String insertContract = "INSERT IGNORE INTO CONTRACT (RECORD_LABEL_ID, ARTIST_ID) VALUES (?, ?)";
            try (PreparedStatement ps = connection.prepareStatement(insertContract)) {
                for (int j = 0; j < rowCount; j++) {
                    ps.setString(1, jazzData[9][j]);     
                    ps.setString(2, jazzData[0][j]);     
                    ps.executeUpdate();
                }
            }

            /* [i][j] , i = column, j = row */
            System.out.printf("Data population completed. Populated %d rows of data\n", rowCount);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //Queries
    public void getArtistWithSongMoreThan3Minutes() {
        try (Connection connection = DriverManager.getConnection(jdbcURL)) {
            Statement statement = connection.createStatement();
            String UseDatabase = "use " + databaseName;
		    statement.executeUpdate(UseDatabase);

            String query = 
            "SELECT ARTIST_ID, ARTIST_NAME " +
            "FROM ARTIST WHERE ARTIST_ID IN ( " + 
                "SELECT ARTIST_ID FROM SONG " +
                "GROUP BY ARTIST_ID " +
                "HAVING AVG(TIME_TO_SEC(SONG_LENGTH)) > 180 " +
            ");";

            ResultSet resultSet = statement.executeQuery(query);
            System.out.println("1. Artist(s) who have wrote songs with an average length greater than 3 minutes:");
            System.out.println("-".repeat(50));
            System.out.printf("%-22s | %s\n", "ARTIST_ID", "ARTIST_NAME");
            System.out.println("-".repeat(50));
            while (resultSet.next()) {
                System.out.printf("%s | %s\n", 
                resultSet.getString("ARTIST_ID"), 
                resultSet.getString("ARTIST_NAME"));
            }
            System.out.println("-".repeat(50));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void getArtistWithMoreThan5Albums() {
        try (Connection connection = DriverManager.getConnection(jdbcURL)) {
            Statement statement = connection.createStatement();
            String UseDatabase = "use " + databaseName;//Before querying I need to select the database...
		    statement.executeUpdate(UseDatabase);

            String query = 
            "SELECT ALBUM.ARTIST_ID, ARTIST.ARTIST_NAME, COUNT(*) AS ALBUM_NUMBER " +
            "FROM ALBUM, ARTIST " + 
            "WHERE ALBUM.ARTIST_ID = ARTIST.ARTIST_ID " +
            "GROUP BY ALBUM.ARTIST_ID " +
            "HAVING COUNT(*) > 6;";

            ResultSet resultSet = statement.executeQuery(query);
            System.out.println("\n2. Artist(s) who own more than 6 albums: ");
            System.out.println("-".repeat(60));
            System.out.printf("%-22s | %-17s | %-10s\n", "ARTIST_ID", "ARTIST_NAME", "ALBUM_NUMBER");
            System.out.println("-".repeat(60));
            while (resultSet.next()) {
                System.out.printf("%-22s | %-17s | %5d\n", 
                resultSet.getString("ARTIST_ID"), 
                resultSet.getString("ARTIST_NAME"),
                resultSet.getInt("ALBUM_NUMBER"));
            }
            System.out.println("-".repeat(60));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void getArtistCountWithRecordLabel() {
        try (Connection connection = DriverManager.getConnection(jdbcURL)) {
            Statement statement = connection.createStatement();
            String UseDatabase = "use " + databaseName;//Before querying I need to select the database...
		    statement.executeUpdate(UseDatabase);

            String query = 
            "SELECT C.RECORD_LABEL_ID, R.RECORD_LABEL_NAME, COUNT(C.ARTIST_ID) AS TOTAL_NUMBER_OF_ARTISTS_SIGNED " +
            "FROM CONTRACT C, RECORD_LABEL R " + 
            "WHERE C.RECORD_LABEL_ID = R.RECORD_LABEL_ID " +
            "GROUP BY C.RECORD_LABEL_ID, R.RECORD_LABEL_NAME;";

            ResultSet resultSet = statement.executeQuery(query);
            System.out.println("\n3. Number of Artist(s) that each record label has signed: ");
            System.out.println("-".repeat(90));
            System.out.printf("%-15s | %-35s | %-10s\n", "RECORD_LABEL_ID", "RECORD_LABEL_NAME", "TOTAL_NUMBER_OF_ARTISTS_SIGNED");
            System.out.println("-".repeat(90));
            while (resultSet.next()) {
                System.out.printf("%-15s | %-35s | %15d\n", 
                resultSet.getString("RECORD_LABEL_ID"), 
                resultSet.getString("RECORD_LABEL_NAME"), 
                resultSet.getInt("TOTAL_NUMBER_OF_ARTISTS_SIGNED"));
            }
            System.out.println("-".repeat(90));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //Deletion
    public void deleteArtistWithLessThan5Albums() {
        try (Connection connection = DriverManager.getConnection(jdbcURL)) {
            Statement statement = connection.createStatement();
            String UseDatabase = "use " + databaseName;
		    statement.executeUpdate(UseDatabase);

            String query = 
            "DELETE FROM ARTIST A WHERE A.ARTIST_ID IN ( " +
            "SELECT ALBUM.ARTIST_ID " +
            "FROM ALBUM " +
            "GROUP BY ALBUM.ARTIST_ID " +
            "HAVING COUNT(*) < 6 )";

            System.out.println("\n4. Deleting Artist(s) with less than 6 Albums");
            int rowsAffected = statement.executeUpdate(query);
            if (rowsAffected == 0) {
                System.out.println("No artist was deleted as they all have 6 or more albums.");
            } else {
                System.out.println(rowsAffected + " artist(s) with less than 6 albums deleted successfully.");
            }        
        } catch (SQLException e) {
            System.out.println("=".repeat(90));
            System.out.println("ON DELETE RESTRICT!! CANNOT DELETE ARTIST");
            System.out.println("=".repeat(90));
        }
    }

    public void deleteAlbumWithAverageSongDurationMoreThan3Minutes() {
        try (Connection connection = DriverManager.getConnection(jdbcURL)) {
            Statement statement = connection.createStatement();
            String UseDatabase = "use " + databaseName;
		    statement.executeUpdate(UseDatabase);

            String query = 
            "DELETE FROM ALBUM WHERE ALBUM.ALBUM_ID IN (" +
                "SELECT ALBUM_SONGS.ALBUM_ID FROM SONG S, ALBUM_SONGS " +
                "WHERE ALBUM_SONGS.SONG_ID = S.SONG_ID " +
                "GROUP BY ALBUM_SONGS.ALBUM_ID " +
                "HAVING AVG(TIME_TO_SEC(S.SONG_LENGTH)) < 180" +
            ");";


            System.out.println("\n5. Deleting Album(s) with average Song Duration less than 3 minutes");
            int rowsAffected = statement.executeUpdate(query);
            if (rowsAffected == 0) {
                System.out.println("No album was deleted as they all have an average song duration greater than 3 minutes.");
            } else {
                System.out.println(rowsAffected + " album(s) with an average song duration less than 3 minutes deleted successfully.");
            }   
        } catch (SQLException e) {
            System.out.println("=".repeat(90));
            System.out.println("ON DELETE RESTRICT!! CANNOT DELETE ALBUM");
            System.out.println("=".repeat(90));
        }
    }

    public static void main(String[] args) {
        String jazz = "38993171.csv";
        JiaJunDatabase JiaJunReader = new JiaJunDatabase();
        System.out.println("Reading the " + jazz + " now");
        String[][] jazzData = JiaJunReader.readCSVFile(jazz);
        JiaJunReader.createTable();
        JiaJunReader.populateTable(jazzData);

        System.out.println("\n\n");
        System.out.println("=".repeat(100));
        System.out.printf("%65s\n", "STARTING EXECUTING QUERIES NOW...");
        System.out.println("=".repeat(100));
        System.out.println("\n\n");

        //Query 1
        JiaJunReader.getArtistWithSongMoreThan3Minutes();
        //Query 2
        JiaJunReader.getArtistWithMoreThan5Albums();
        //Query 3
        JiaJunReader.getArtistCountWithRecordLabel();

        //Deletion 1
        JiaJunReader.deleteArtistWithLessThan5Albums();
        //Deletion 2
        JiaJunReader.deleteAlbumWithAverageSongDurationMoreThan3Minutes();
    }
}

class MiguelDatabase {
    private static final String DB_URL = "jdbc:mysql://localhost:3306";
    private static final String DB_NAME = "student_38751852";
    private Statement stmt;
    private Connection conn;

    public MiguelDatabase() throws SQLException {
        createDatabase();
        if (conn == null) {
            System.out.println("Error: Connection to the database could not be established.");
            return;
        }
        createTables();
        parseCSVAndInsertData("38751852.csv");

        deleteNationality(1);
        deleteSinger(1);

        query1();
        query2();

        conn.close();
    }

    private void createDatabase() {
        try {
            conn = DriverManager.getConnection(DB_URL);
            this.stmt = conn.createStatement();
            stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS " + DB_NAME);
            System.out.println("Database " + DB_NAME + " created or already exists.");

            conn = DriverManager.getConnection(DB_URL + "/" + DB_NAME);
            this.stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void createTables() throws SQLException {
        String createSingerTable = "CREATE TABLE IF NOT EXISTS Singer (" +
                "SingerID INT AUTO_INCREMENT PRIMARY KEY," +
                "StageName VARCHAR(255) NOT NULL," +
                "RealName VARCHAR(255)," +
                "NationalityID INT NOT NULL," +
                "DebutYear YEAR," +
                "FOREIGN KEY (NationalityID) REFERENCES Nationality(NationalityID) ON DELETE RESTRICT);";

        String createAlbumTable = "CREATE TABLE IF NOT EXISTS Album (" +
                "AlbumID INT AUTO_INCREMENT PRIMARY KEY," +
                "Title VARCHAR(255) NOT NULL," +
                "ReleaseDate DATE," +
                "SingerID INT NOT NULL," +
                "FOREIGN KEY (SingerID) REFERENCES Singer(SingerID) ON DELETE RESTRICT);";

        String createSongTable = "CREATE TABLE IF NOT EXISTS Song (" +
                "SongID INT AUTO_INCREMENT PRIMARY KEY," +
                "Title VARCHAR(255) NOT NULL," +
                "ReleaseDate DATE," +
                "SingerID INT NOT NULL," +
                "FOREIGN KEY (SingerID) REFERENCES Singer(SingerID) ON DELETE RESTRICT)";

        String createNationalityTable = "CREATE TABLE IF NOT EXISTS Nationality (" +
                "NationalityID INT AUTO_INCREMENT PRIMARY KEY," +
                "CountryName VARCHAR(255) NOT NULL," +
                "MusicStreamers INT);";

        stmt.executeUpdate(createNationalityTable);
        stmt.executeUpdate(createSingerTable);
        stmt.executeUpdate(createAlbumTable);
        stmt.executeUpdate(createSongTable);
    }

    private void parseCSVAndInsertData(String csvFilePath) {
        String line;
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] data = line.split(",");
                if (!data[12].equals("null")) {
                    String insertNationality = "INSERT INTO Nationality (NationalityID, CountryName, MusicStreamers) VALUES (?, ?, ?)";
                    try (PreparedStatement pstmt = conn.prepareStatement(insertNationality)) {
                        pstmt.setInt(1, Integer.parseInt(data[12]));
                        pstmt.setString(2, data[13]);
                        pstmt.setInt(3, Integer.parseInt(data[14]));
                        pstmt.executeUpdate();
                    }
                }

                if (!data[7].equals("null")) {
                    String insertSinger = "INSERT INTO Singer (SingerID, StageName, RealName, NationalityID, DebutYear) VALUES (?, ?, ?, ?, ?)";
                    try (PreparedStatement pstmt = conn.prepareStatement(insertSinger)) {
                        pstmt.setString(1, data[7]);
                        pstmt.setString(2, data[8]);
                        pstmt.setString(3, data[9]);
                        pstmt.setInt(4, Integer.parseInt(data[10]));
                        pstmt.setString(5, data[11]);
                        pstmt.executeUpdate();
                    }
                }

                if (!data[3].equals("null")) {
                    String insertAlbum = "INSERT INTO Album (AlbumID, Title, ReleaseDate, SingerID) VALUES (?, ?, ?, ?)";
                    try (PreparedStatement pstmt = conn.prepareStatement(insertAlbum)) {
                        pstmt.setString(1, data[3]);
                        pstmt.setString(2, data[4]);
                        pstmt.setDate(3, new Date(dateFormat.parse(data[5]).getTime()));
                        pstmt.setInt(4, Integer.parseInt(data[6]));
                        pstmt.executeUpdate();
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                }

                if (!data[0].equals("null")) {
                    String insertSong = "INSERT INTO Song (SongID, Title, SingerID) VALUES (?, ?, ?)";
                    try (PreparedStatement pstmt = conn.prepareStatement(insertSong)) {
                        pstmt.setInt(1, Integer.parseInt(data[0]));
                        pstmt.setString(2, data[1]);
                        pstmt.setInt(3, Integer.parseInt(data[2]));
                        pstmt.executeUpdate();
                    }
                }
            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    public void deleteNationality(int nationalityID) {
        System.out.println("Deleting Nationality");
        String deleteNationality = "DELETE FROM Nationality WHERE NationalityID = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(deleteNationality)) {
            pstmt.setInt(1, nationalityID);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void deleteSinger(int singerID) {
        System.out.println("Deleting Singer");
        String deleteSinger = "DELETE FROM Singer WHERE SingerID = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(deleteSinger)) {
            pstmt.setInt(1, singerID);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void query1() throws SQLException {
        System.out.println("Countries with more than 1,000,000 music streamers.");
        String query = "SELECT CountryName, SUM(MusicStreamers) AS TotalMusicStreamers " +
                "FROM Nationality " +
                "GROUP BY CountryName " +
                "HAVING TotalMusicStreamers > 1000000";
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            System.out.println(rs.getString("CountryName"));
        }
    }

    public void query2() throws SQLException {
        System.out.println("Singers with more than 1 album.");
        String query = "SELECT Singer.StageName," +
                "COUNT(Album.AlbumID) AS NumberOfAlbums " +
                "FROM Singer " +
                "JOIN Album ON Singer.SingerID = Album.SingerID " +
                "GROUP BY Singer.SingerID " +
                "HAVING COUNT(Album.AlbumID) > 1";

        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            System.out.println(rs.getString("StageName"));
        }
    }
}

