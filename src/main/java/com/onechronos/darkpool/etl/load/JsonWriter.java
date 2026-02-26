package com.onechronos.darkpool.etl.load;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.core.JsonGenerator;
import com.onechronos.darkpool.etl.exception.JsonWriterException;
import com.onechronos.darkpool.etl.model.CleanedTradeRecord;
import com.onechronos.darkpool.etl.model.ExceptionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Class that allows writing CleanedTradeRecord and ExceptionRecord to two JSON files simultaneously.
 */
public class JsonWriter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(JsonWriter.class);

    private final ObjectMapper mapper;
    private final JsonGenerator cleanedTradesGenerator;
    private final JsonGenerator exceptionsGenerator;

    private JsonWriter(Path cleanedTradesPath, Path exceptionsPath) throws IOException {
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule()) // Serializes Instant to ISO-8601 format
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .setDateFormat(new StdDateFormat().withColonInTimeZone(true));

        Files.createDirectories(cleanedTradesPath.getParent());
        Files.createDirectories(exceptionsPath.getParent());

        this.cleanedTradesGenerator = mapper.createGenerator(cleanedTradesPath.toFile(), JsonEncoding.UTF8);
        this.exceptionsGenerator = mapper.createGenerator(exceptionsPath.toFile(), JsonEncoding.UTF8);

        cleanedTradesGenerator.useDefaultPrettyPrinter();
        exceptionsGenerator.useDefaultPrettyPrinter();


        cleanedTradesGenerator.writeStartArray();
        exceptionsGenerator.writeStartArray();

        log.info("Opened output files: {}, {}", cleanedTradesPath, exceptionsPath);
    }

    /**
     * Create JsonWriter
     */
    public static JsonWriter open(Path cleanedTradesPath, Path exceptionsPath) throws IOException {
        return new JsonWriter(cleanedTradesPath, exceptionsPath);
    }

    /**
     * Write CleanedTradeRecord to JSON file
     */
    public void writeCleanedTrade(CleanedTradeRecord trade) throws JsonWriterException {
        try {
            mapper.writeValue(cleanedTradesGenerator, trade);
        } catch (IOException e) {
            throw new JsonWriterException("Failure while writing JSON cleaned trade", e);
        }
    }

    /**
     * Write ExceptionRecord to JSON file
     */
    public void writeException(ExceptionRecord exception) throws JsonWriterException {
        try {
            mapper.writeValue(exceptionsGenerator, exception);
        } catch (IOException e) {
            throw new JsonWriterException("Failure while writing JSON exception record", e);
        }
    }

    @Override
    public void close() throws IOException {
        closeGenerator(cleanedTradesGenerator, "cleaned trades");
        closeGenerator(exceptionsGenerator, "exceptions");
    }

    private void closeGenerator(JsonGenerator generator, String name) throws IOException {
        try {
            generator.writeEndArray();
            generator.close();
        } catch (IOException e) {
            log.error("Failed to close {} file", name, e);
            throw e;
        }
    }
}