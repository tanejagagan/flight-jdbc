package info.gtaneja.flight.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public record FetchInstruction(String connectionId,
                               String[] sqls,
                               int fetchSize) {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public byte[] serialize() throws JsonProcessingException {
        return objectMapper.writeValueAsBytes(this);
    }

    public static FetchInstruction deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, FetchInstruction.class);
    }
}
