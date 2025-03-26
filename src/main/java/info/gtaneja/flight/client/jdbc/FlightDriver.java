package info.gtaneja.flight.client.jdbc;

import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Base64;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

public class FlightDriver implements Driver {

    static {
        try {
            DriverManager.registerDriver(new FlightDriver());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static String generateBasicAuthHeader(String username, String password) {
        byte[] up = Base64.getEncoder().encode((username + ":" + password).getBytes(StandardCharsets.UTF_8));
        return Auth2Constants.BASIC_PREFIX +
                new String(up);
    }

    private static FlightClient createClient(BufferAllocator allocator, Location location, String username, String password) {
        return FlightClient.builder(allocator, location).intercept(new FlightClientMiddleware.Factory() {
            @Override
            public FlightClientMiddleware onCallStarted(CallInfo info) {

                return new FlightClientMiddleware() {
                    private volatile String jwt = null;

                    @Override
                    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
                        if (jwt == null) {
                            outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER,
                                    generateBasicAuthHeader(username, password));
                        } else {
                            outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER,
                                    generateJWTAuthHeader(jwt));
                        }
                    }

                    @Override
                    public void onHeadersReceived(CallHeaders incomingHeaders) {
                        jwt = incomingHeaders.get(Auth2Constants.AUTHORIZATION_HEADER);
                    }

                    @Override
                    public void onCallCompleted(CallStatus status) {

                    }
                };
            }
        }).build();
    }

    private static String generateJWTAuthHeader(String jwt) {
        return Auth2Constants.BEARER_PREFIX + jwt;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if(!acceptsURL(url)) {
            throw new SQLException("unsupported url");
        }
        String username = "";
        String password = "";
        URI uri = URI.create(url.substring("jdbc:".length()));
        Location location = Location.forGrpcInsecure(uri.getHost(), uri.getPort());
        BufferAllocator allocator = new RootAllocator();
        FlightClient flightClient= createClient(allocator, location, username, password );
        UUID connectionId = UUID.randomUUID();
        return new FlightConnection(connectionId, flightClient, allocator);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith("jdbc:grpc:");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
