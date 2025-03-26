package info.gtaneja.flight.client.jdbc;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.memory.BufferAllocator;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executor;

public class FlightConnection implements Connection {

    private final FlightClient flightClient;
    private final BufferAllocator allocator;
    private final UUID connectionId;

    public FlightConnection(UUID connectionId, FlightClient flightClient, BufferAllocator allocator) {
        this.flightClient = flightClient;
        this.allocator = allocator;
        this.connectionId = connectionId;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new FlightStatement(flightClient, connectionId);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public void commit() throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public void rollback() throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public void close() throws SQLException {
        try {
            this.flightClient.doAction(new Action("close", connectionId.toString().getBytes()));
            this.flightClient.close();
        } catch (InterruptedException e) {
            throw new SQLException(e);
        }
        this.allocator.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public String getSchema() throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("unsupported");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("unsupported");
    }
}
