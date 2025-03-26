package info.gtaneja.flight.server;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
import java.net.URISyntaxException;

public class Main {

    public static void main(String[] args) throws URISyntaxException {
        var location = new Location(args[0]);
        _main(location);
    }

    public static void _main(Location location) {
        BufferAllocator allocator = new RootAllocator();
        FlightProducer flightProducer = new DuckDBFlightProducer(location, allocator);
        FlightServer flightServer = FlightServer.builder(allocator, location, flightProducer)
                .build();
        var serverThread = new Thread(() -> {
            try {
                flightServer.start();
                System.out.println("S1: Server (Location): Listening on port " + flightServer.getPort());
                flightServer.awaitTermination();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        serverThread.start();
    }
}
