// WordCountJob — minimal Kafka Streams word-count used to benchmark against
// the custom Go MapReduce framework.
//
// Reads lines from topic "bench-input", splits them into lowercase
// alphanumeric tokens, counts each word, and writes (word, count) updates to
// topic "bench-output".
//
// Self-timing: the job starts a watchdog thread that polls the consumer-group
// lag on the input topic. When every partition has been fully processed
// (lag == 0, observed twice in a row), the job prints a machine-parseable
//   TIMING compute_seconds=<float>
// line (time from Streams start to lag-zero) and exits. This mirrors the
// "compute_seconds" metric reported by the Go orchestrator: input production
// is excluded, processing time is included.
//
// Build (no Maven needed — uses the jars bundled with the Kafka distribution):
//   javac -cp "$KAFKA_HOME/libs/*" WordCountJob.java
// Run:
//   java -cp "$KAFKA_HOME/libs/*:." WordCountJob <bootstrap-server> <application-id>
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class WordCountJob {

    static final String INPUT_TOPIC = "bench-input";
    static final String OUTPUT_TOPIC = "bench-output";

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: WordCountJob <bootstrap-server> <application-id>");
            System.exit(2);
        }
        final String bootstrap = args[0];
        final String appId = args[1];
        final String stateDir = "/tmp/kafka-bench/streams-state/" + appId.replaceAll("[^A-Za-z0-9._-]", "_");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        // Larger batches: fairer comparison with the batch-oriented Go system.
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 64 * 1024 * 1024L);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> lines =
                builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, Long> counts = lines
                .flatMapValues(line -> Arrays.stream(
                                line.toLowerCase(Locale.ROOT).split("[^\\p{L}\\p{N}]+"))
                        .filter(w -> !w.isEmpty())
                        .collect(Collectors.toList()))
                .groupBy((key, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("bench-counts"));

        counts.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        CountDownLatch done = new CountDownLatch(1);

        long t0 = System.nanoTime();
        streams.start();

        // Watchdog: exit once the consumer group has fully caught up with the
        // input topic (lag == 0 across all partitions, seen twice in a row).
        // Retries on transient broker disconnects rather than crashing.
        Thread watchdog = new Thread(() -> {
            Map<String, Object> adminConf = new HashMap<>();
            adminConf.put("bootstrap.servers", bootstrap);
            adminConf.put("request.timeout.ms", "30000");
            adminConf.put("default.api.timeout.ms", "30000");
            try (Admin admin = Admin.create(adminConf)) {
                int zeroStreak = 0;
                int errorStreak = 0;
                while (zeroStreak < 2) {
                    Thread.sleep(2000);
                    try {
                        long lag = totalLag(admin, appId);
                        if (lag == 0) {
                            zeroStreak++;
                        } else {
                            zeroStreak = 0;
                        }
                        errorStreak = 0;
                    } catch (Exception e) {
                        errorStreak++;
                        System.err.println("Watchdog: transient error (" + errorStreak + "): " + e.getMessage());
                        if (errorStreak > 30) {
                            // ~60 s of sustained errors — broker is truly gone
                            e.printStackTrace();
                            System.exit(1);
                        }
                    }
                }
                double seconds = (System.nanoTime() - t0) / 1e9;
                System.out.printf("TIMING compute_seconds=%.3f%n", seconds);
                done.countDown();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        });
        watchdog.setDaemon(true);
        watchdog.start();

        done.await();
        streams.close(Duration.ofSeconds(30));
    }

    /** Total lag of the streams app's consumer group on the input topic. */
    static long totalLag(Admin admin, String appId) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committed =
                admin.listConsumerGroupOffsets(appId)
                        .partitionsToOffsetAndMetadata().get();
        Set<TopicPartition> inputParts = committed.keySet().stream()
                .filter(tp -> tp.topic().equals(INPUT_TOPIC))
                .collect(Collectors.toSet());
        if (inputParts.isEmpty()) {
            return Long.MAX_VALUE; // group not ready yet
        }
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends =
                admin.listOffsets(inputParts.stream()
                                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest())))
                        .all().get();
        long lag = 0;
        for (TopicPartition tp : inputParts) {
            long end = ends.get(tp).offset();
            long cur = committed.get(tp).offset();
            lag += Math.max(0, end - cur);
        }
        return lag;
    }
}
