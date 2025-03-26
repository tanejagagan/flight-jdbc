package info.gtaneja.flight.server;

import info.gtaneja.flight.common.FetchInstruction;
import io.github.tanejagagan.sql.commons.ConnectionPool;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;

public class DuckDBFlightProducer extends NoOpFlightProducer implements Closeable {

    private final BufferAllocator allocator;
    public DuckDBFlightProducer(Location location,
                                BufferAllocator allocator){
        this.allocator = allocator;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener){
        byte[] bytes = ticket.getBytes();
        try {
            FetchInstruction fetchInstruction = FetchInstruction.deserialize(bytes);
            String[] sqls = fetchInstruction.sqls();
            try(DuckDBConnection connection = ConnectionPool.getConnection()) {
                for(int i =0 ; i < sqls.length -1 ; i ++) {
                    ConnectionPool.execute(connection, sqls[i]);
                }
                try(BufferAllocator allocator1 = allocator.newChildAllocator(fetchInstruction.connectionId(), 0, Long.MAX_VALUE);
                    ArrowReader reader = ConnectionPool.getReader(connection, allocator1, sqls[sqls.length - 1], fetchInstruction.fetchSize());
                    VectorSchemaRoot root  = reader.getVectorSchemaRoot()) {
                    listener.start(root);
                    while (reader.loadNextBatch()) {
                        listener.putNext();
                    }
                    listener.completed();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {

    }
}
