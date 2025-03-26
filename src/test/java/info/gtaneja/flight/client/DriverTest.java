package info.gtaneja.flight.client;

import info.gtaneja.flight.client.jdbc.FlightDriver;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DriverTest {
    @Test
    public void parseTest() {
        String url = "jdbc:flight://a:80,b:90,c:100/db1?x=y;a=b";
        List<?> resultList = FlightDriver.parseUrl(url);
    }

    @Test
    public void parseUnixSocket() {
        String url = "jdbc:unix:///tmp/my_grpc_socket";
        List<?> resultList = FlightDriver.parseUrl(url);
        assertEquals (new FlightDriver.UrlParseResult("unix", "localhost", -1, "/tmp/my_grpc_socket", null),
                resultList.get(0));
    }
}
