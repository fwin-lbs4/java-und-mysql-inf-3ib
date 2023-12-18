package quest.flo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;

/**
 * Class that creates an object to abstract management of the database.
 *
 * @author Florian.WINDISCH
 */
public class Database {
    // Logger for the Database-Class.
    private static final Logger log = LogManager.getLogger(Database.class);

    // The Connection to the database.
    private final Connection connection;

    /**
     * Constructor for the Database object.
     *
     * @param url      The url to use for the Connection, should not include the database.
     * @param user     The user for accessing the database.
     * @param password The password for accessing the database.
     * @throws SQLException If the connection to the dbms fails.
     */
    public Database(String url, String user, String password) throws SQLException {
        log.info("Connecting to Database with url: " + url);

        try {
            this.connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            String errorMessage = "Failed connecting to Database: " + e.getMessage();

            log.error(errorMessage);

            throw new SQLException(errorMessage);
        }
    }

    /**
     * Execute a "USE (database)" statement.
     *
     * @param name Name of the database to use.
     * @throws SQLException If switching the database fails.
     */
    public void use(String name) throws SQLException {
        try {
            Statement statement = this.connection.createStatement();

            statement.execute("USE " + name);
        } catch (SQLException e) {
            String errorMessage = "Failed to use " + name + ": " + e.getMessage();

            log.error(errorMessage);

            throw new SQLException(errorMessage);
        }
    }

    /**
     * Create a database if it does not exist.
     * Does not drop the database if it does exist.
     *
     * @param name Name of the database to create.
     * @throws SQLException If creating the database fails.
     */
    public void createDatabase(String name) throws SQLException {
        this.createDatabase(name, false);
    }

    /**
     * Create a database if it does not exist.
     * You can specify if it should first drop the database if it does exist.
     *
     * @param name         Name of the database to create.
     * @param dropIfExists If the database should be dropped first.
     * @throws SQLException If creating the database fails.
     */
    public void createDatabase(String name, boolean dropIfExists) throws SQLException {
        try {
            Statement statement = this.connection.createStatement();

            if (dropIfExists) {
                statement.execute("DROP DATABASE IF EXISTS " + name);
            }

            statement.execute("CREATE DATABASE IF NOT EXISTS " + name);
        } catch (SQLException e) {
            String errorMessage = "Failed creating Database " + name + ": " + e.getMessage();

            log.error(errorMessage);

            throw new SQLException(errorMessage);
        }
    }

    /**
     * Create a table if it does not exist.
     * Does not drop the table if it does exist.
     *
     * @param name Name of the table to create.
     * @param cols Array of columns to use to create the table.
     * @throws SQLException If creating the table fails.
     */
    public void createTable(String name, String[] cols) throws SQLException {
        this.createTable(name, cols, false);
    }

    /**
     * Create a table if it does not exist.
     * You can specify if it should first drop the table if it does exist.
     *
     * @param name         Name of the table to create.
     * @param cols         Array of columns to use to create the table.
     * @param dropIfExists If the table should be dropped first.
     * @throws SQLException If creating the table fails.
     */
    public void createTable(String name, String[] cols, boolean dropIfExists) throws SQLException {
        String colsString = "(" + String.join(",", cols) + ")";

        try {
            Statement statement = this.connection.createStatement();

            if (dropIfExists) {
                statement.execute("DROP TABLE IF EXISTS " + name);
            }

            statement.execute("CREATE TABLE IF NOT EXISTS " + name + " " + colsString);
        } catch (SQLException e) {
            String errorMessage = "Failed creating Table " + name + ": " + e.getMessage();

            log.error(errorMessage);

            throw new SQLException(errorMessage);
        }
    }

    /**
     * Return a PreparedStatement so queries can be done.
     *
     * @param statement An SQL statement that can include parameter placeholders.
     * @return The PreparedStatement with the pre-compiled SQL-Statement.
     * @throws SQLException If preparing the Statement fails.
     */
    public PreparedStatement prepareStatement(String statement) throws SQLException {
        return this.connection.prepareStatement(statement);
    }

    /**
     * Get the Connection to the database.
     * For example: to use for methods directly on the Connection.
     *
     * @return The Connection to the Database.
     */
    public Connection getConnection() {
        return this.connection;
    }

    /**
     * Close the database connection.
     *
     * @throws SQLException If closing the database connection fails.
     */
    public void disconnect() throws SQLException {
        log.info("Disconnecting from Database");

        try {
            this.connection.close();
        } catch (SQLException e) {
            String errorMessage = "Failed closing DB-Connection: " + e.getMessage();

            log.error(errorMessage);

            throw new SQLException(errorMessage);
        }
    }
}
