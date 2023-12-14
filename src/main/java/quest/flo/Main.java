package quest.flo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;

/**
 * Testing-Implementation for the trains-system.
 *
 * @author Florian.WINDISCH
 */
public class Main {
    // Logger for the Main-class.
    private static final Logger log = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Start");

        try {
            Database db = new Database("jdbc:mariadb://localhost/", "root", null);

            Trains trains = new Trains(db);
            Route[] routes = trains.updateRoutes();

            for (Route route : routes) {
                System.out.println(route);
            }

            db.disconnect();
        } catch (SQLException e) {
            String errorMessage = "Something went wrong: " + e.getMessage();
            log.error(errorMessage);
            System.out.println(errorMessage);
        }
    }
}