import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BlobTransferWithChunking {

    private static final String sqlServerUrl = "jdbc:sqlserver://<SQL_SERVER_HOST>:<PORT>;databaseName=<DATABASE_NAME>;user=<USER>;password=<PASSWORD>";
    private static final String oracleUrl = "jdbc:oracle:thin:@<ORACLE_HOST>:<PORT>:<SID>";
    private static final int CHUNK_SIZE = 100; // Define the size of each chunk

    public static void main(String[] args) {
        try {
            migrateBlobData();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void migrateBlobData() throws SQLException {
        int lastProcessedId = getLastProcessedId(); // Implement this method to retrieve the last successfully migrated ID

        try (
            Connection sqlServerConnection = DriverManager.getConnection(sqlServerUrl);
            Connection oracleConnection = DriverManager.getConnection(oracleUrl)
        ) {
            oracleConnection.setAutoCommit(false);
            
            while (true) {
                try (
                    PreparedStatement psSource = sqlServerConnection.prepareStatement("SELECT id, blob_column FROM source_table WHERE id > ? ORDER BY id ASC LIMIT ?");
                    PreparedStatement psTarget = oracleConnection.prepareStatement("INSERT INTO target_table (id, blob_column) VALUES (?, ?)")
                ) {
                    psSource.setInt(1, lastProcessedId);
                    psSource.setInt(2, CHUNK_SIZE);

                    try (ResultSet rs = psSource.executeQuery()) {
                        if (!rs.next()) {
                            break; // Exit loop if no more records to process
                        }
                        
                        do {
                            int id = rs.getInt("id");
                            Blob blob = rs.getBlob("blob_column");

                            psTarget.setInt(1, id);
                            psTarget.setBlob(2, blob);
                            psTarget.executeUpdate();

                            lastProcessedId = id; // Update last processed ID
                        } while (rs.next());

                        oracleConnection.commit(); // Commit after each chunk
                        updateLastProcessedId(lastProcessedId); // Implement this method to update the tracking of the last processed ID
                    }
                } catch (SQLException e) {
                    oracleConnection.rollback(); // Rollback in case of an exception
                    throw e;
                }
            }

            System.out.println("BLOB data migration completed successfully.");
        }
    }

    private static int getLastProcessedId() {
        // Implement logic to retrieve the last successfully migrated ID from a persistent store
        // This could be a file, database table, etc.
        return 0; // Placeholder return value
    }

    private static void updateLastProcessedId(int id) {
        // Implement logic to update the last successfully migrated ID in a persistent store
        // This ensures progress is saved and can be resumed from the last point in case of failure
    }
}
