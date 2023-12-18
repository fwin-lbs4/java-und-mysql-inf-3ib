package quest.flo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.Scanner;

/**
 * Testing-Implementation for the trains-system.
 *
 * @author Florian.WINDISCH
 */
public class Main {
    // Logger for the Main-class.
    private static final Logger log = LogManager.getLogger(Main.class);

    /**
     * Empty default Constructor.
     */
    public Main() {
    }

    /**
     * main methode.
     *
     * @param args Args to pass to the program.
     */
    public static void main(String[] args) {
        log.info("Start");

        try {
            Database db = new Database("jdbc:mariadb://localhost/", "root", null);

            Trains trains = new Trains(db);

            try (Scanner scan = new Scanner(System.in)) {
                Boolean createRoute = null;
                do {
                    System.out.println("Do you want to create a new Route (y|n)?");
                    String selected = scan.next();

                    switch (selected) {
                        case "y", "Y" -> createRoute = true;
                        case "n", "N" -> createRoute = false;
                    }
                } while (createRoute == null);

                if (createRoute) {
                    System.out.println(trains.createRoute(scan));
                }
            }

            trains.updateRoutes();

            System.out.println(trains);

            db.disconnect();
        } catch (SQLException e) {
            String errorMessage = "Something went wrong: " + e.getMessage();
            log.error(errorMessage);
            System.out.println(errorMessage);
        }
    }
}