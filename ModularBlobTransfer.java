import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ModularBlobTransfer {

    private static final String sqlServerUrl = "jdbc:sqlserver://<SQL_SERVER_HOST>:<PORT>;databaseName=<DATABASE_NAME>;user=<USER>;password=<PASSWORD>";
    private static final String oracleUrl = "jdbc:oracle:thin:@<ORACLE_HOST>:<PORT>:<SID>";
    private static final int CHUNK_SIZE = 100;

    public static void main(String[] args) {
        try {
            new ModularBlobTransfer().startMigration();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void startMigration() throws SQLException {
        int lastProcessedId = getLastProcessedId();
        try (
            Connection sqlServerConnection = createConnection(sqlServerUrl);
            Connection oracleConnection = createConnection(oracleUrl)
        ) {
            migrateBlobData(sqlServerConnection, oracleConnection, lastProcessedId);
        }
    }

    private Connection createConnection(String url) throws SQLException {
        return DriverManager.getConnection(url);
    }

    private void migrateBlobData(Connection sqlServerConnection, Connection oracleConnection, int lastProcessedId) throws SQLException {
        oracleConnection.setAutoCommit(false);

        String sourceQuery = "SELECT id, blob_column FROM source_table WHERE id > ? ORDER BY id ASC LIMIT ?";
        String targetQuery = "INSERT INTO target_table (id, blob_column) VALUES (?, ?)";

        while (true) {
            try (
                PreparedStatement psSource = sqlServerConnection.prepareStatement(sourceQuery);
                PreparedStatement psTarget = oracleConnection.prepareStatement(targetQuery)
            ) {
                psSource.setInt(1, lastProcessedId);
                psSource.setInt(2, CHUNK_SIZE);

                try (ResultSet rs = psSource.executeQuery()) {
                    if (!rs.next()) {
                        break; // No more records to process
                    }
                    do {
                        int id = rs.getInt("id");
                        Blob blob = rs.getBlob("blob_column");

                        psTarget.setInt(1, id);
                        psTarget.setBlob(2, blob);
                        psTarget.executeUpdate();

                        lastProcessedId = id;
                    } while (rs.next());

                    oracleConnection.commit();
                    updateLastProcessedId(lastProcessedId);
                }
            } catch (SQLException e) {
                oracleConnection.rollback();
                throw e;
            }
        }
        System.out.println("BLOB data migration completed successfully.");
    }

    private int getLastProcessedId() {
        // Implement retrieval logic
        return 0;
    }

    private void updateLastProcessedId(int id) {
        // Implement update logic
    }
}
