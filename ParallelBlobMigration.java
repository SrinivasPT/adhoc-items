import java.sql.*;

public class ParallelBlobMigration {

    private static final String sqlServerUrl = "jdbc:sqlserver://<SQL_SERVER_HOST>:<PORT>;databaseName=<DATABASE_NAME>;user=<USER>;password=<PASSWORD>";
    private static final String oracleUrl = "jdbc:oracle:thin:@<ORACLE_HOST>:<PORT>:<SID>";
    private static final int CHUNK_SIZE = 100;
    private static final int THREAD_COUNT = 4; // Adjust based on your data and environment

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        for (int i = 0; i < THREAD_COUNT; i++) {
            int threadId = i;
            executor.submit(() -> {
                try {
                    migrateBlobDataInParallel(threadId);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("Parallel BLOB data migration completed.");
    }

    private static void migrateBlobDataInParallel(int threadId) throws SQLException {
        try (
            Connection sqlServerConnection = DriverManager.getConnection(sqlServerUrl);
            Connection oracleConnection = DriverManager.getConnection(oracleUrl)
        ) {
            oracleConnection.setAutoCommit(false);
            int offset = threadId * CHUNK_SIZE;
            
            while (true) {
                try (
                    PreparedStatement psSource = sqlServerConnection.prepareStatement(
                        "SELECT id, blob_column FROM source_table WHERE MOD(id, ?) = ? ORDER BY id ASC LIMIT ?");
                    PreparedStatement psTarget = oracleConnection.prepareStatement(
                        "INSERT INTO target_table (id, blob_column) VALUES (?, ?)")
                ) {
                    psSource.setInt(1, THREAD_COUNT);
                    psSource.setInt(2, threadId);
                    psSource.setInt(3, CHUNK_SIZE);

                    ResultSet rs = psSource.executeQuery();
                    if (!rs.next()) {
                        oracleConnection.commit();
                        break; // Exit loop if no more records to process
                    }

                    do {
                        int id = rs.getInt("id");
                        Blob blob = rs.getBlob("blob_column");

                        psTarget.setInt(1, id);
                        psTarget.setBlob(2, blob);
                        psTarget.executeUpdate();
                    } while (rs.next());

                    oracleConnection.commit(); // Commit after processing each chunk
                } catch (SQLException e) {
                    oracleConnection.rollback(); // Rollback in case of an exception
                    throw e;
                }
            }
        }
    }
}
