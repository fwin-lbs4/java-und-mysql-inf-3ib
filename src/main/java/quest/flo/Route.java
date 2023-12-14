package quest.flo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Class to create a Route object.
 *
 * @author Florian.WINDISCH
 */
public class Route {
    // Logger for the Route-Class
    private static final Logger log = LogManager.getLogger(Route.class);

    // The Route-id.
    private final int id;

    // The identifying number of the train on this Route.
    private final int trainNr;

    // The type of the train used on this Route.
    private final String trainType;

    // The starting Platform.
    private final Platform arrival;

    // The ending Platform.
    private final Platform departure;

    /**
     * Constructor for the Route object.
     * Holds the train that is used for this route, the stations on the route and arrival/departure times.
     *
     * @param db        Database object, used to query the data.
     * @param dbName    The name of the database to use.
     * @param id        The id of the Route.
     * @param arrival   Timestamp of arrival.
     * @param departure Timestamp of departure.
     * @param trainNr   The identification number of the train.
     * @param direction Is the train on a return trip.
     * @throws SQLException If getting info for the train or creating the Platform objects fails.
     */
    public Route(
            Database db, String dbName, int id, Timestamp departure, Timestamp arrival, int trainNr, boolean direction
    ) throws SQLException {
        this.id = id;
        this.trainNr = trainNr;
        this.trainType = this.getTrainType(db, dbName);

        log.info("Creating route " + id + " for train " + this.trainType + " " + trainNr + " in " + (direction ? "forwards" : "reverse") + " direction");

        this.arrival = new Platform(db, dbName, trainNr, direction, true, arrival);
        this.departure = new Platform(db, dbName, trainNr, direction, false, departure);
    }

    /**
     * Get the type of the train from the Database.
     *
     * @param db     Database used to select the train-type.
     * @param dbName Name of the database to use.
     * @return The train-type.
     * @throws SQLException If selecting the data fails.
     */
    private String getTrainType(Database db, String dbName) throws SQLException {
        db.use(dbName);

        try {
            PreparedStatement statement = db.prepareStatement(
                    "SELECT tt.name as 'type' FROM traintype tt LEFT JOIN train t ON t.traintype_idtraintype = tt.idtraintype WHERE tt.idtraintype = ? LIMIT 1");
            statement.setInt(1, this.trainNr);

            ResultSet results = statement.executeQuery();

            results.first();

            String type = results.getString("type");

            results.close();

            return type;
        } catch (SQLException e) {
            String errorMessage = "Failed selecting train-types: " + e.getMessage();

            log.error(errorMessage);

            throw new SQLException(errorMessage);
        }
    }

    /**
     * Convert the Route to a string.
     *
     * @return A string representation of the Route.
     */
    @Override
    public String toString() {
        return String.join("\n", new String[]{
                "Route: " + this.id,
                "Train: " + this.trainType + " " + this.trainNr,
                "Departure: " + this.departure,
                "Arrival: " + this.arrival
        }) + "\n";
    }
}
