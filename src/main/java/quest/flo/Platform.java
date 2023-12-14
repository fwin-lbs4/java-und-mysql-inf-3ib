package quest.flo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Class to create a Platform object.
 *
 * @author Florian.WINDISCH
 */
public class Platform {
    // Logger for the Platform-Class.
    private static final Logger log = LogManager.getLogger(Platform.class);

    // The Platform number.
    private final int nr;

    // The station the Platform is in.
    private final String station;

    // The city the Platform is in.
    private final String city;

    // The arrival/departure time for this Platform-Instance.
    private final Timestamp time;

    /**
     * Constructor for the Platform object.
     * Holds the arrival/departure time, the platform number, the train station and the city it is in.
     *
     * @param db       Database object to select data concerning the platform.
     * @param dbName   Name of the database to use.
     * @param trainId  Identification number of the train.
     * @param forwards Is the train on a return trip.
     * @param getStart Is this the starting or ending station of this Route.
     * @param time     The arrival/departure time.
     * @throws SQLException If selecting the data failed.
     */
    public Platform(
            Database db, String dbName, int trainId, boolean forwards, boolean getStart, Timestamp time
    ) throws SQLException {
        log.info("Finding " + (getStart ? "starting" : "ending") + " platform for train: " + trainId);

        this.time = time;

        db.use(dbName);

        PreparedStatement statement = db.prepareStatement(
                "SELECT p.nr as 'nr', s.name as 'station', c.name as 'city' FROM train_has_platform t LEFT JOIN platform p on t.platform_idplatform = p.idplatform LEFT JOIN station s on p.station_idstation = s.idstation LEFT JOIN city c on s.idstation = c.station_idstation WHERE t.train_nrtrain = ? AND t.start = ? LIMIT 1");
        statement.setInt(1, trainId);
        statement.setBoolean(2, (getStart && forwards) || (!getStart && !forwards));

        ResultSet results = statement.executeQuery();

        results.first();

        this.nr = results.getInt("nr");
        this.station = results.getString("station");
        this.city = results.getString("city");

        results.close();
    }

    /**
     * Convert the Platform to a string.
     *
     * @return A string representation of the Platform.
     */
    @Override
    public String toString() {
        return "platform " + this.nr + " from station " + this.station + " in " + this.city + " at " + this.time;
    }
}
