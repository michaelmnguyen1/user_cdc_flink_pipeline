package flink_job.com.autodesk.exercise.service;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;

public class UserCdcPipeline {

    // ======================
    // Entity classes are normally put under their own package as in
	// flink_job.com.autodesk.exercise.entity
    // ======================
    public static class User {
        public String userId;
        public String name;
        public long createdTimestamp;
        public long lastModifiedTimestamp;

        public User() {}
        public User(String userId, String name) {
            this.userId = userId;
            this.name = name;
        }
    }

    public static class EnrichedUser extends User {
        public String enrichedData;
        public long eventTs;
        public String op;
    }

    public static class CdcEvent {
        public String op; // c, u, d
        public User before;
        public User after;
        public long timestamp;

        public String getUserId() {
            return after != null ? after.userId : before.userId;
        }
    }

    // ======================
    // CdcEventDeserializer is used to deserialize CDC Events received from Kafka
    // ======================
    public static class CdcEventDeserializer implements DeserializationSchema<CdcEvent> {
		private static final long serialVersionUID = 1L;
		private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public CdcEvent deserialize(byte[] message) throws IOException {
            return mapper.readValue(message, CdcEvent.class);
        }

        @Override
        public boolean isEndOfStream(CdcEvent nextElement) { 
        	return false; 
        }

        @Override
        public TypeInformation<CdcEvent> getProducedType() { 
        	return TypeInformation.of(CdcEvent.class); 
        }
    }

    public static void main(String[] args) throws Exception {
    	UserCdcPipeline userCdcPipeline;
    	try {
    		userCdcPipeline = new UserCdcPipeline();
    		userCdcPipeline.runPipeline(args);
    	} catch (Throwable exc) {
    		exc.printStackTrace();    		
    		// To do: error handling
    	}    	
    }

    public void runPipeline(String[] args) throws Exception {
    	
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000); // 1 min checkpoint

        // ------------------------
        // 1) Kafka Source
        // ------------------------
        KafkaSource<CdcEvent> kafkaSource = KafkaSource.<CdcEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("user_cdc_topic")
                .setGroupId("cdc-user-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new CdcEventDeserializer())
                .build();

        DataStream<CdcEvent> cdcStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.<CdcEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((e, ts) -> e.timestamp),
                "Kafka User CDC Source"
        );

        // ------------------------
        // 2) Key by userId to ensure all CDC events for the same user are processed together in one Flink task.
        // The event producer is responsible for putting different CDC events for the same user
        // on the same Kafka partition and in the right order. This is based on the premise 
        // of providing the solution as far upstream as possible.
        // ------------------------
        KeyedStream<CdcEvent, String> keyedStream = cdcStream.keyBy(CdcEvent::getUserId);

        // ------------------------
        // 3) Enrich events
        // ------------------------
        DataStream<EnrichedUser> enrichedUserStream = keyedStream.process(
                new KeyedProcessFunction<String, CdcEvent, EnrichedUser>() {
					private static final long serialVersionUID = 1L;

					@Override
                    public void processElement(CdcEvent event, Context ctx, 
                    		Collector<EnrichedUser> enrichedUserCollector) {
                        EnrichedUser enrichedUser = new EnrichedUser();
                        User eventUser = event.after != null ? event.after : event.before;

                        enrichedUser.userId = eventUser.userId;
                        enrichedUser.name = eventUser.name;
                        enrichedUser.createdTimestamp = eventUser.createdTimestamp;
                        enrichedUser.lastModifiedTimestamp = eventUser.lastModifiedTimestamp;
                        enrichedUser.op = event.op;
                        enrichedUser.eventTs = event.timestamp;
                        
                        /*
                         * 3. For data enrichment, for this exercise, keep it super simple with 
                         * hard-coded data. For production, data enrichment
   							would likely involve data look-up. To minimize such latency and 
   							for performance and scalability, in production 
   							consider	either
   							1. Storing such look-up data in an in-memory database such as Apache Ignite
   							2. Prefetching the data from the database into Redis cache.
   
   							Option 1 is preferred, because there is no extra infrastructure 
   							and there is no stale cache issue.

                         */
                        enrichedUser.enrichedData = "ENRICHED-" + enrichedUser.userId;

                        enrichedUserCollector.collect(enrichedUser);
                    }
                }
        );

        // ------------------------
        // 4) Sink to MySQL (TwoPhaseCommittingSink) in sync with Flink's checkpointing 
        // to ensure exactly once 
        // ------------------------
        enrichedUserStream.sinkTo(new UserJdbcTwoPhaseSink(
        		// In production code, retrieve these values from AWS Secret manager.
                "jdbc:mysql://localhost:3306/autodesk_db",
                "root",
                "password"
        ));

        env.execute("User CDC Pipeline with TwoPhaseCommittingSink");
    }

    // ======================
    // TwoPhaseCommittingSink
    // database operation classes are typically put under their own package as in
    // flink_job.com.autodesk.exercise.dao
    // ======================

    /** Transaction object for one checkpoint */
    static class JdbcTransaction {
        final List<EnrichedUser> enrichedUsers = new ArrayList<>();
    }

    /** Writer – buffers events per checkpoint */
    static class UserJdbcSinkWriter implements SinkWriter<EnrichedUser> {
        private JdbcTransaction currentTxn;

        public UserJdbcSinkWriter(String jdbcUrl, String username, String password) {
        	// jdbcUrl,  username, password are not used here.
            this.currentTxn = new JdbcTransaction();
        }

        @Override
        public void write(EnrichedUser element, Context context) {
            currentTxn.enrichedUsers.add(element);
        }

        /** Called by Flink at checkpoint – returns the snapshot for commit */
        public Collection<JdbcTransaction> prepareCommit(boolean flush) {
            JdbcTransaction snapshotTxn = currentTxn;
            currentTxn = new JdbcTransaction(); // new txn for next checkpoint
            return Collections.singletonList(snapshotTxn);
        }

        // flush and close are no-op because the DB operations of the CDC events are not
        // performed until Flink does its checkpoint. At that time, JdbcCommitter is
        // used to perform the DB operations in that checkpoint's interval as 
        // one transaction
        @Override
        public void flush(boolean endOfInput) { 
        	/* no-op */ 
        }

        @Override
        public void close() {
        	/* no-op */ 
        }
    }

    /** Commits transactions to Database i.e.  MySQL */
    static class JdbcCommitter implements Committer<JdbcTransaction> {
    	static long runCount = 0;
    	
        final int MAX_RETRIES = 5;
        final long INITIAL_BACKOFF_MS = 500;
        final double BACKOFF_MULTIPLIER = 2.0;
        
        @Override
        public void commit(Collection<CommitRequest<JdbcTransaction>> requests) throws IOException {
            for (CommitRequest<JdbcTransaction> request : requests) {
                persist(request.getCommittable());
            }
        }

        @Override
        public void close() {
            // DB resources such as Connection and PreparedStatement are scoped within persist method
            // so there are resources to close here. 
        	/* no-op */ 
        }

        private void persist(JdbcTransaction txn) throws IOException {
            runCount++;
            long persistStartNs = System.nanoTime();
            long totalPersistDurationMs, commitDurationMs, commitStartNs = 0;
            int totalSqlRetries = 0;
            int commitRetries = 0;

            try (Connection conn = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/autodesk_db", "root", "password");
                 PreparedStatement createStmt = conn.prepareStatement(
                         "INSERT INTO enriched_user(user_id,name,created_timestamp,last_modified_timestamp,enriched_data) VALUES(?,?,?,?,?)");
                 PreparedStatement updateStmt = conn.prepareStatement(
                         "UPDATE enriched_user SET name=?, last_modified_timestamp=?, enriched_data=? WHERE user_id=?");
                 PreparedStatement deleteStmt = conn.prepareStatement(
                         "DELETE FROM enriched_user WHERE user_id=?")) {

                conn.setAutoCommit(false);

                // -----------------------------------
                // Execute statements
                // -----------------------------------
                for (EnrichedUser enrichedUser : txn.enrichedUsers) {
                    int attempt = 0;
                    long backoff = INITIAL_BACKOFF_MS;

                    while (true) {
                        try {
                            switch (enrichedUser.op) {
                                case "c":
                                    createStmt.setString(1, enrichedUser.userId);
                                    createStmt.setString(2, enrichedUser.name);
                                    createStmt.setLong(3, enrichedUser.createdTimestamp);
                                    createStmt.setLong(4, enrichedUser.lastModifiedTimestamp);
                                    createStmt.setString(5, enrichedUser.enrichedData);
                                    createStmt.executeUpdate();
                                    break;
                                    
                                case "u":
                                    updateStmt.setString(1, enrichedUser.name);
                                    updateStmt.setLong(2, enrichedUser.lastModifiedTimestamp);
                                    updateStmt.setString(3, enrichedUser.enrichedData);
                                    updateStmt.setString(4, enrichedUser.userId);
                                    updateStmt.executeUpdate();
                                    break;
                                    
                                case "d":
                                    deleteStmt.setString(1, enrichedUser.userId);
                                    deleteStmt.executeUpdate();
                                    break;
                                    
                                default:
                                    throw new SQLException("Unknown op: " + enrichedUser.op);
                            }
                            break; // success
                        } catch (SQLException exc) {
                        	// in Production, emit the errors to CLoudWatch or log them, so they can be monitored by tools such as Splunk.
                        	exc.printStackTrace();
                        	
                            attempt++;
                            totalSqlRetries++;

                            if (attempt >= MAX_RETRIES) {
                                throw new IOException(
                                        "SQL failed after retries for user=" + enrichedUser.userId, exc);
                            }

                            try {
                                Thread.sleep(backoff);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                throw new IOException("Interrupted during SQL backoff", ie);
                            }
                            backoff *= BACKOFF_MULTIPLIER;
                        }
                    }
                }
                long recordDurationMs = (System.nanoTime() - persistStartNs) / 1_000_000;

                // In production: emit metric instead of println
                System.out.println(
                        "runCount = " + runCount + " - Record processed user = " + txn.enrichedUsers.size() +
                        " - durationMs = " + recordDurationMs);

                // -----------------------------------
                // Commit with retry
                // -----------------------------------
                commitStartNs = System.nanoTime();
                long commitBackoff = INITIAL_BACKOFF_MS;

                for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
                    try {
                        conn.commit();
                        break;
                    } catch (SQLException e) {
                        commitRetries++;
                        if (attempt == MAX_RETRIES) {
                            throw new IOException("Commit failed after retries", e);
                        }
                        try {
                            Thread.sleep(commitBackoff);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Interrupted during commit backoff", ie);
                        }
                        commitBackoff *= BACKOFF_MULTIPLIER;
                    }
                }

                commitDurationMs =
                        (System.nanoTime() - commitStartNs) / 1_000_000;

                totalPersistDurationMs =
                        (System.nanoTime() - persistStartNs) / 1_000_000;

                // -----------------------------------
                // metrics log aligned with checkpoint
                // In production, 
                // 1. Emit the metrics to CloudWatch or log them so tools such as Splunk can provide operational trends of the pipeline.
                // 2. Store the metrics in a table call user_cdc_pipeline_metrics for historical long-term analysis.
                // -----------------------------------
                System.out.println(
                		"runCount = " + runCount + " - DB COMMIT SUCCESS" +
                        " - records = " + txn.enrichedUsers.size() +
                        " - totalDurationMs =" + totalPersistDurationMs +
                        " - commitDurationMs =" + commitDurationMs +
                        " - sqlRetries = " + totalSqlRetries +
                        " - commitRetries = " + commitRetries
                );

            } catch (SQLException exc) {
                commitDurationMs =
                        (System.nanoTime() - commitStartNs) / 1_000_000;

                totalPersistDurationMs =
                        (System.nanoTime() - persistStartNs) / 1_000_000;
                // -----------------------------------
                // metrics log aligned with checkpoint
                // In production, 
                // 1. Emit the metrics to CloudWatch or log them so tools such as Splunk can provide operational trends of the pipeline.
                // 2. Store the metrics in a table call user_cdc_pipeline_metrics for historical long-term analysis.
                // -----------------------------------
            	exc.printStackTrace();
                System.err.println(
                		"runCount = " + runCount + " - DB COMMIT FAILED" +
                        " - records = " + txn.enrichedUsers.size() +
                        " - totalDurationMs =" + totalPersistDurationMs +
                        " - commitDurationMs =" + commitDurationMs +
                        " - sqlRetries = " + totalSqlRetries +
                        " - commitRetries = " + commitRetries
                );
            	
                throw new IOException("Failed to persist JDBC transaction", exc);
            }
        }
    }

    /** TwoPhaseCommittingSink */
    static class UserJdbcTwoPhaseSink implements Sink<EnrichedUser> {
		private static final long serialVersionUID = 1L;
		private final String jdbcUrl;
        private final String username;
        private final String password;

        public UserJdbcTwoPhaseSink(String jdbcUrl, String username, String password) {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
        }

        @Override
        public SinkWriter<EnrichedUser> createWriter(InitContext context) {
            return new UserJdbcSinkWriter(jdbcUrl, username, password);
        }

        public Optional<Committer<JdbcTransaction>> createCommitter() {
            return Optional.of(new JdbcCommitter());
        }

        public Optional<SimpleVersionedSerializer<JdbcTransaction>> getSerializer() {
            return Optional.empty();
        }
    }
}
