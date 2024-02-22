import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class BlobTransfer {

    public static void main(String[] args) {
        String sqlServerUrl = "jdbc:sqlserver://<SQL_SERVER_HOST>:<PORT>;databaseName=<DATABASE_NAME>;user=<USER>;password=<PASSWORD>";
        String oracleUrl = "jdbc:oracle:thin:@<ORACLE_HOST>:<PORT>:<SID>";

        String sqlServerQuery = "SELECT blob_column FROM source_table WHERE condition = ?";
        String oracleInsertQuery = "INSERT INTO target_table (blob_column) VALUES (?)";

        try (
            Connection sqlServerConnection = DriverManager.getConnection(sqlServerUrl);
            Connection oracleConnection = DriverManager.getConnection(oracleUrl);
            PreparedStatement psSource = sqlServerConnection.prepareStatement(sqlServerQuery);
            PreparedStatement psTarget = oracleConnection.prepareStatement(oracleInsertQuery);
        ) {
            // Set any parameters for the SQL Server query
            psSource.setString(1, "yourConditionValue");

            try (ResultSet rs = psSource.executeQuery()) {
                while (rs.next()) {
                    Blob blob = rs.getBlob("blob_column");

                    // Assuming the target column in Oracle is also a BLOB
                    psTarget.setBlob(1, blob);
                    psTarget.executeUpdate();
                }
            }

            System.out.println("BLOB data transfer completed successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
