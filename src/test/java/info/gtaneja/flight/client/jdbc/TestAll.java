package info.gtaneja.flight.client.jdbc;


import info.gtaneja.flight.server.Main;
import org.apache.arrow.flight.Location;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.sql.*;

public class TestAll {

    private static int port;

    private static final String testSql =
            "select e, e%2=0, cast(e as string) str from (" +
                    "SELECT generate_series as e FROM generate_series(2000))";

    @BeforeAll
    public static void beforeAll() throws IOException {
        port = getRandomPort();
        Location location = Location.forGrpcInsecure("localhost", port);
        Main._main(location);
    }
    @Test
    void testRunningSql() throws SQLException, ClassNotFoundException {
        try ( Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
            statement.execute(testSql);
            try(ResultSet resultSet = statement.getResultSet()){
                while (resultSet.next()){
                    long l = resultSet.getLong(1);
                    boolean b = resultSet.getBoolean(2);
                    String string = resultSet.getString(3);
                    System.out.printf("%s,%s,%s\n", l, b, string);
                }
            }
        }
    }

    public static Connection getConnection() throws SQLException, ClassNotFoundException {
        Class.forName(FlightDriver.class.getName());
        return DriverManager.getConnection("jdbc:grpc://localhost:" + port );
    }

    public static int getRandomPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
