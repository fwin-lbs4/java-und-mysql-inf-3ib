package quest.flo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Class that creates a train system. Sets up the Database and can get the Routes.
 *
 * @author Florian.WINDISCH
 */
public class Trains {
    // Logger for the Trains-class.
    private static final Logger log = LogManager.getLogger(Trains.class);

    // Database object to use in the system.
    private final Database db;

    // The database to use for this system.
    private final String dbName = "trains";

    // List of Routes the system currently knows about.
    private final Map<Integer, Route> routes = new HashMap<>();

    /**
     * Constructor for Trains object.
     *
     * @param db Database object, used to manipulate the db.
     * @throws SQLException If any of the queries or throw an error.
     */
    public Trains(Database db) throws SQLException {
        log.info("Initializing train system!");

        this.db = db;


        // Check if the db exists
        boolean dbExists;

        try (PreparedStatement stmt = this.db.prepareStatement(
                "SELECT count(*) as 'dbExists' FROM `information_schema`.`schemata` WHERE `schema_name` = '" + this.dbName + "'")) {
            ResultSet rs = stmt.executeQuery();
            rs.first();

            dbExists = rs.getBoolean("dbExists");

            rs.close();
        }

        // If the db does not exist create it.
        if (!dbExists) {
            this.db.createDatabase(this.dbName);
        }

        this.db.use(this.dbName);


        // Check if the tables exist!
        int counter;

        try (PreparedStatement stmt = this.db.prepareStatement(
                "SELECT count(*) as 'count' FROM `information_schema`.`tables` WHERE `table_schema` = '" + this.dbName + "' AND `table_name` IN ('city', 'platform', 'route', 'station', 'train', 'traintype', 'train_has_platform');")) {
            ResultSet rs = stmt.executeQuery();
            rs.first();

            counter = rs.getInt("count");

            rs.close();
        }

        // If the amount of tables does not match the expected amount recreate all tables.
        if (counter != 7) {
            log.warn("Not enough tables, creating Tables!");
            this.createTables();
            this.insertInitialData();
        }
    }

    /**
     * Get the current Routes from the database.
     *
     * @throws SQLException If querying for routes failed.
     */
    public void updateRoutes() throws SQLException {
        this.db.use(this.dbName);

        try (PreparedStatement statement = this.db.prepareStatement(
                "SELECT idroute, arrival, departure, train_nrtrain,direction FROM route")) {

            ResultSet results = statement.executeQuery();

            while (results.next()) {
                int routeId = results.getInt("idroute");
                this.routes.put(routeId, new Route(
                        this.db,
                        this.dbName,
                        routeId,
                        results.getTimestamp("departure"),
                        results.getTimestamp("arrival"),
                        results.getInt("train_nrtrain"),
                        results.getBoolean("direction")
                ));
            }
        }
    }

    /**
     * Getter for the Routes in the object.
     *
     * @return An array of Routes, not fresh from the database.
     */
    public Map<Integer, Route> getRoutes() {
        return this.routes;
    }

    /**
     * Get a specific Route from the Routes.
     *
     * @param routeId The id of the Route to get.
     * @return The Route that was requested.
     */
    public Route getRoute(Integer routeId) {
        return this.routes.get(routeId);
    }

    /**
     * Insert a new Route into the Database and then return the resulting Route.
     *
     * @param scan Scanner to handle Input.
     * @return The new Route that was inserted.
     * @throws SQLException If there was an issue inserting into the Database.
     */
    public Route createRoute(Scanner scan) throws SQLException {
        Integer trainNr = this.chooseTrain(scan);
        Map<Integer, String[]> platforms = this.getPlatforms(trainNr);
        Boolean direction = this.chooseDirection(platforms, scan);
        Timestamp departure = this.chooseTimestamp(true, scan);
        Timestamp arrival = this.chooseTimestamp(false, scan);
        int insertedRoute;

        try (PreparedStatement statement = this.db.getConnection().prepareStatement(
                "INSERT INTO route (arrival, departure, direction, train_nrtrain) VALUES (?, ?, ?, ?)",
                Statement.RETURN_GENERATED_KEYS
        )) {
            statement.setTimestamp(1, arrival);
            statement.setTimestamp(2, departure);
            statement.setBoolean(3, direction);
            statement.setInt(4, trainNr);

            statement.executeUpdate();

            try (ResultSet rs = statement.getGeneratedKeys()) {
                rs.first();

                insertedRoute = rs.getInt(1);
            }
        }

        this.updateRoutes();

        return this.routes.get(insertedRoute);
    }

    /**
     * Ask the user to enter a timestamp for the Route.
     *
     * @param start Are we asking for the departure or arrival Timestamp?
     * @param scan  Scanner to handle Input.
     * @return The Timestamp the user has entered.
     */
    private Timestamp chooseTimestamp(Boolean start, Scanner scan) {
        Timestamp timestamp = null;

        do {
            System.out.println("Please input a " + (start ? "departure" : "arrival") + " timestamp (yyyy-mm-dd hh:mm):");

            try {
                Thread.sleep(1000);
                String timeString = scan.nextLine() + ":00";
                timestamp = Timestamp.valueOf(timeString);
            } catch (IllegalArgumentException | InterruptedException ignored) {
                System.out.println("Erroneous timestamp entered, try again!");
            }
        } while (timestamp == null);

        return timestamp;
    }

    /**
     * Choose the direction to use for the Route.
     *
     * @param platforms A Map of platforms.
     * @param scan      Scanner to handle Input.
     * @return True if the direction is forwards, false if it is backwards.
     */
    private Boolean chooseDirection(Map<Integer, String[]> platforms, Scanner scan) {
        Boolean chosen = null;
        Map<Boolean, String> directions = new HashMap<>() {{
            put(true, " --> ");
            put(false, " --> ");
        }};

        for (int platform : platforms.keySet()) {
            String[] val = platforms.get(platform);

            if (val[1].equals("0")) {
                directions.put(true, val[0] + directions.get(true));
                directions.put(false, directions.get(false) + val[0]);
            }

            if (val[1].equals("1")) {
                directions.put(true, directions.get(true) + val[0]);
                directions.put(false, val[0] + directions.get(false));
            }
        }

        for (Boolean direction : directions.keySet()) {
            System.out.println((direction ? "Forwards (f): " : "Backwards (b): ") + directions.get(direction));
        }

        do {
            System.out.println("Choose a direction! (f|b)");
            String selected = scan.next();

            switch (selected) {
                case "f", "F" -> chosen = true;
                case "b", "B" -> chosen = false;
            }
        } while (!directions.containsKey(chosen));

        return chosen;
    }

    /**
     * Get the platforms from the Database.
     *
     * @param trainNr The trainNr to get the platforms for.
     * @return A Map of platforms.
     * @throws SQLException If selecting the platforms fails.
     */
    private Map<Integer, String[]> getPlatforms(Integer trainNr) throws SQLException {
        this.db.use(this.dbName);

        Map<Integer, String[]> platforms = new HashMap<>();

        try (PreparedStatement statement = this.db.prepareStatement(
                "SELECT p.nr as 'nr', s.name as 'station', c.name as 'city', t.start as 'start' FROM train_has_platform t LEFT JOIN platform p on t.platform_idplatform = p.idplatform LEFT JOIN station s on p.station_idstation = s.idstation LEFT JOIN city c on s.idstation = c.station_idstation WHERE t.train_nrtrain = ?")) {
            statement.setInt(1, trainNr);
            ResultSet rs = statement.executeQuery();

            while (rs.next()) {
                int nr = rs.getInt("nr");
                String[] details = {
                        rs.getString("station") + " " + nr + " " + rs.getString("city"), rs.getString("start")
                };

                platforms.put(nr, details);
            }
        }

        return platforms;
    }

    /**
     * Choosing the train to use for the new Route.
     *
     * @param scan Scanner to handle Input.
     * @return The number of the chosen train.
     * @throws SQLException If there is an issue getting the trains from the database.
     */
    private Integer chooseTrain(Scanner scan) throws SQLException {
        this.db.use(this.dbName);

        Map<Integer, String> trains = this.getTrains();
        Integer chosen = null;

        for (int train : trains.keySet()) {
            System.out.println(train + "-->" + trains.get(train));
        }

        do {
            if (chosen != null) System.out.println("Invalid train number...");
            System.out.println("Choose a train!");

            chosen = scan.nextInt();
        } while (!trains.containsKey(chosen));

        return chosen;
    }

    /**
     * Get a list of trains from the database.
     *
     * @return A Map of trains.
     * @throws SQLException If there was an error selecting the trains from the database.
     */
    private Map<Integer, String> getTrains() throws SQLException {
        this.db.use(this.dbName);

        try (PreparedStatement statement = this.db.prepareStatement(
                "SELECT t.nrtrain as 'trainNr', tt.name as 'name' FROM train t LEFT JOIN traintype tt ON tt.idtraintype = t.traintype_idtraintype")) {
            ResultSet rs = statement.executeQuery();

            Map<Integer, String> trains = new HashMap<>();

            while (rs.next()) {
                trains.put(rs.getInt("trainNr"), rs.getString("name"));
            }

            return trains;
        }
    }

    /**
     * Create the tables for the train system.
     *
     * @throws SQLException If any of the creation statements fail.
     */
    private void createTables() throws SQLException {
        this.db.use(this.dbName);

        try {
            this.db.createTable("station", new String[]{
                    "`idstation` INT NOT NULL AUTO_INCREMENT",
                    "`name` VARCHAR(45) NOT NULL",
                    "PRIMARY KEY (`idstation`)"
            });

            this.db.createTable("city", new String[]{
                    "`idcity` INT NOT NULL AUTO_INCREMENT",
                    "`name` VARCHAR(45) NOT NULL",
                    "`station_idstation` INT NOT NULL",
                    "PRIMARY KEY (`idcity`)",
                    "INDEX `fk_city_station1_idx` (`station_idstation` ASC)",
                    "CONSTRAINT `fk_city_station1` FOREIGN KEY (`station_idstation`) REFERENCES `trains`.`station` (`idstation`) ON DELETE NO ACTION ON UPDATE NO ACTION"
            });

            this.db.createTable("platform", new String[]{
                    "`idplatform` INT NOT NULL AUTO_INCREMENT",
                    "`nr` VARCHAR(45) NOT NULL",
                    "`station_idstation` INT NOT NULL",
                    "PRIMARY KEY (`idplatform`)",
                    "INDEX `fk_platform_station1_idx` (`station_idstation` ASC)",
                    "CONSTRAINT `fk_platform_station1` FOREIGN KEY (`station_idstation`) REFERENCES `trains`.`station` (`idstation`) ON DELETE NO ACTION ON UPDATE NO ACTION"
            });

            this.db.createTable("traintype", new String[]{
                    "`idtraintype` INT NOT NULL AUTO_INCREMENT",
                    "`name` VARCHAR(45) NOT NULL",
                    "PRIMARY KEY (`idtraintype`)"
            });

            this.db.createTable("train", new String[]{
                    "`nrtrain` INT NOT NULL AUTO_INCREMENT",
                    "`traintype_idtraintype` INT NOT NULL",
                    "`acquisition` DATE NOT NULL",
                    "PRIMARY KEY (`nrtrain`)",
                    "INDEX `fk_train_traintype_idx` (`traintype_idtraintype` ASC)",
                    "CONSTRAINT `fk_train_traintype` FOREIGN KEY (`traintype_idtraintype`) REFERENCES `trains`.`traintype` (`idtraintype`) ON DELETE NO ACTION ON UPDATE NO ACTION "
            });

            this.db.createTable("train_has_platform", new String[]{
                    "`train_nrtrain` INT NOT NULL",
                    "`platform_idplatform` INT NOT NULL",
                    "`start` TINYINT NOT NULL DEFAULT 0",
                    "PRIMARY KEY (`train_nrtrain`, `platform_idplatform`)",
                    "INDEX `fk_train_has_platform_platform1_idx` (`platform_idplatform` ASC)",
                    "INDEX `fk_train_has_platform_train1_idx` (`train_nrtrain` ASC)",
                    "CONSTRAINT `fk_train_has_platform_train1` FOREIGN KEY (`train_nrtrain`) REFERENCES `trains`.`train` (`nrtrain`) ON DELETE NO ACTION ON UPDATE NO ACTION",
                    "CONSTRAINT `fk_train_has_platform_platform1` FOREIGN KEY (`platform_idplatform`) REFERENCES `trains`.`platform` (`idplatform`) ON DELETE NO ACTION ON UPDATE NO ACTION"
            });

            this.db.createTable("route", new String[]{
                    "`idroute` INT NOT NULL AUTO_INCREMENT",
                    "`arrival` TIMESTAMP(6) NOT NULL DEFAULT NOW()",
                    "`departure` TIMESTAMP(6) NOT NULL DEFAULT NOW()",
                    "`train_nrtrain` INT NOT NULL",
                    "`direction` TINYINT NOT NULL DEFAULT 0",
                    "PRIMARY KEY (`idroute`)",
                    "INDEX `fk_route_train1_idx` (`train_nrtrain` ASC)",
                    "CONSTRAINT `fk_route_train1` FOREIGN KEY (`train_nrtrain`) REFERENCES `trains`.`train` (`nrtrain`) ON DELETE NO ACTION ON UPDATE NO ACTION"
            });
        } catch (SQLException e) {
            String errorMessage = "Failed creating tables: " + e.getMessage();
            log.error(errorMessage);
            throw new SQLException(errorMessage);
        }
    }

    /**
     * Insert the initial data for the train system.
     *
     * @throws SQLException If inserting the initial data failed.
     */
    private void insertInitialData() throws SQLException {
        this.db.use(this.dbName);

        this.insertStations();
        this.insertCities();
        this.insertPlatforms();
        this.insertTrainTypes();
        this.insertTrains();
        this.insertTrainHasPlatform();
        this.insertRoutes();
    }

    /**
     * Insert the stations.
     *
     * @throws SQLException If inserting the stations fails.
     */
    private void insertStations() throws SQLException {
        try (PreparedStatement statement = this.db.prepareStatement(
                "INSERT INTO station (idstation, name) VALUES (?, ?)")) {
            Map<Integer, String> stations = new HashMap<>() {{
                put(1, "hbf-salzburg");
                put(2, "hbf-wien");
                put(3, "hbf-linz");
            }};

            for (int key : stations.keySet()) {
                statement.setInt(1, key);
                statement.setString(2, stations.get(key));

                statement.addBatch();
            }

            statement.executeBatch();
        }
    }

    /**
     * Insert the cities.
     *
     * @throws SQLException If inserting the cities fails.
     */
    private void insertCities() throws SQLException {
        try (PreparedStatement statement = this.db.prepareStatement(
                "INSERT INTO city (idcity, name, station_idstation) VALUES (?, ?, ?)")) {

            Map<Integer, String> cities = new HashMap<>() {{
                put(1, "salzburg");
                put(2, "wien");
                put(3, "linz");
            }};

            for (int key : cities.keySet()) {
                statement.setInt(1, key);
                statement.setString(2, cities.get(key));
                statement.setInt(3, key);

                statement.addBatch();
            }

            statement.executeBatch();
        }
    }

    /**
     * Insert the platforms.
     *
     * @throws SQLException If inserting the platforms fails.
     */
    private void insertPlatforms() throws SQLException {
        try (PreparedStatement statement = this.db.prepareStatement(
                "INSERT INTO platform (idplatform, nr, station_idstation) VALUES (?, ?, ?)")) {

            Map<Integer, Integer[]> platforms = new HashMap<>() {{
                put(1, new Integer[]{1, 1});
                put(2, new Integer[]{2, 1});
                put(3, new Integer[]{1, 2});
                put(4, new Integer[]{2, 2});
                put(5, new Integer[]{1, 3});
                put(6, new Integer[]{2, 3});
            }};

            for (int key : platforms.keySet()) {
                Integer[] val = platforms.get(key);
                statement.setInt(1, key);
                statement.setInt(2, val[0]);
                statement.setInt(3, val[1]);

                statement.addBatch();
            }

            statement.executeBatch();
        }
    }

    /**
     * Insert the train-types.
     *
     * @throws SQLException If inserting the train-types fails.
     */
    private void insertTrainTypes() throws SQLException {
        try (PreparedStatement statement = this.db.prepareStatement(
                "INSERT INTO traintype (idtraintype, name) VALUES (?, ?)")) {

            Map<Integer, String> trainTypes = new HashMap<>() {{
                put(1, "ICE");
                put(2, "S-Bahn");
                put(3, "REX");
            }};

            for (int key : trainTypes.keySet()) {
                statement.setInt(1, key);
                statement.setString(2, trainTypes.get(key));

                statement.addBatch();
            }

            statement.executeBatch();
        }
    }

    /**
     * Insert the trains.
     *
     * @throws SQLException If inserting the trains fails.
     */
    private void insertTrains() throws SQLException {
        try (PreparedStatement statement = this.db.prepareStatement(
                "INSERT INTO train (nrtrain, traintype_idtraintype, acquisition) VALUES (?, ?, ?)")) {

            Map<Integer, Integer> trainsWithType = new HashMap<>() {{
                put(1, 3);
                put(2, 2);
                put(3, 1);
            }};

            Map<Integer, Date> trainsWithAcquisition = new HashMap<>() {{
                put(1, Date.valueOf("2020-09-04"));
                put(2, Date.valueOf("2019-01-05"));
                put(3, Date.valueOf("2021-12-24"));
            }};

            for (int key : trainsWithType.keySet()) {
                statement.setInt(1, key);
                statement.setInt(2, trainsWithType.get(key));
                statement.setDate(3, trainsWithAcquisition.get(key));

                statement.addBatch();
            }

            statement.executeBatch();
        }
    }

    /**
     * Insert the platform-train-relations.
     *
     * @throws SQLException If inserting the platform-train-relations fails.
     */
    private void insertTrainHasPlatform() throws SQLException {
        try (PreparedStatement statement = this.db.prepareStatement(
                "INSERT INTO train_has_platform (train_nrtrain, platform_idplatform, start) VALUES (?, ?, ?)")) {

            Integer[][] trainHasPlatform = {
                    {1, 2, 1}, {1, 3, 0}, {2, 4, 1}, {2, 5, 0}, {3, 1, 0}, {3, 6, 1}
            };

            for (Integer[] index : trainHasPlatform) {
                statement.setInt(1, index[0]);
                statement.setInt(2, index[1]);
                statement.setBoolean(3, index[2] == 1);

                statement.addBatch();
            }

            statement.executeBatch();
        }
    }

    /**
     * Insert the routes.
     *
     * @throws SQLException If inserting the routes fails.
     */
    private void insertRoutes() throws SQLException {
        try (PreparedStatement statement = this.db.prepareStatement(
                "INSERT INTO route (idroute, arrival, departure, train_nrtrain, direction) VALUES (?, ?, ?, ?, ?)")) {

            Map<Integer, Timestamp[]> routeTimes = new HashMap<>() {{
                put(7, new Timestamp[]{
                        Timestamp.valueOf("2023-12-04 09:30:00.000000"), Timestamp.valueOf("2023-12-04 08:00:00.000000")
                });
                put(8, new Timestamp[]{
                        Timestamp.valueOf("2023-12-04 11:30:00.000000"), Timestamp.valueOf("2023-12-04 10:00:00.000000")
                });
                put(9, new Timestamp[]{
                        Timestamp.valueOf("2023-12-04 13:00:00.000000"), Timestamp.valueOf("2023-12-04 12:00:00.000000")
                });
                put(10, new Timestamp[]{
                        Timestamp.valueOf("2023-12-04 14:15:00.000000"), Timestamp.valueOf("2023-12-04 13:15:00.000000")
                });
                put(11, new Timestamp[]{
                        Timestamp.valueOf("2023-12-04 06:30:00.000000"), Timestamp.valueOf("2023-12-04 05:45:00.000000")
                });
                put(12, new Timestamp[]{
                        Timestamp.valueOf("2023-12-04 07:25:00.556000"), Timestamp.valueOf("2023-12-04 06:40:00.493000")
                });
            }};

            Map<Integer, Integer[]> routeRelations = new HashMap<>() {{
                put(7, new Integer[]{1, 1});
                put(8, new Integer[]{1, 0});
                put(9, new Integer[]{2, 1});
                put(10, new Integer[]{2, 0});
                put(11, new Integer[]{3, 1});
                put(12, new Integer[]{3, 0});
            }};

            for (int route : routeTimes.keySet()) {
                Timestamp[] times = routeTimes.get(route);
                Integer[] relations = routeRelations.get(route);

                statement.setInt(1, route);
                statement.setTimestamp(2, times[0]);
                statement.setTimestamp(3, times[1]);
                statement.setInt(4, relations[0]);
                statement.setInt(5, relations[1]);

                statement.addBatch();
            }

            statement.executeBatch();
        }
    }

    /**
     * Convert the Trains-System to a String by converting all the internal Routes to a String.
     *
     * @return A string representation of the Trains-System.
     */
    @Override
    public String toString() {
        if (this.routes.isEmpty()) {
            String warningMessage = "Currently no Routes in the Train-System, please update the Routes!";
            log.warn(warningMessage);
            return warningMessage;
        }

        StringBuilder routesStringBuilder = new StringBuilder();

        for (int route : this.routes.keySet()) {
            routesStringBuilder.append(this.routes.get(route));
        }

        return routesStringBuilder.toString();
    }
}
