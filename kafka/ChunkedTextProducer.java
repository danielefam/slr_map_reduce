import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ChunkedTextProducer {
    static final int DEFAULT_MAX_BYTES = 8 * 1024 * 1024;

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            System.err.println("usage: ChunkedTextProducer <bootstrap-server> <topic> [max-bytes]");
            System.exit(2);
        }
        String bootstrap = args[0];
        String topic = args[1];
        int maxBytes = args.length == 3 ? Integer.parseInt(args[2]) : DEFAULT_MAX_BYTES;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxBytes + 1024 * 1024);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader reader =
                     new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8), 1 << 20)) {
            StringBuilder chunk = new StringBuilder();
            int chunkBytes = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                String record = line + '\n';
                int recordBytes = record.getBytes(StandardCharsets.UTF_8).length;
                if (recordBytes > maxBytes) {
                    throw new IllegalArgumentException("single input line exceeds max-bytes: " + recordBytes);
                }
                if (chunkBytes > 0 && chunkBytes + recordBytes > maxBytes) {
                    producer.send(new ProducerRecord<>(topic, chunk.toString())).get();
                    chunk.setLength(0);
                    chunkBytes = 0;
                }
                chunk.append(record);
                chunkBytes += recordBytes;
            }
            if (chunkBytes > 0) {
                producer.send(new ProducerRecord<>(topic, chunk.toString())).get();
            }
            producer.flush();
        }
    }
}
