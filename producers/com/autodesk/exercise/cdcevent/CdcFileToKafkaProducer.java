package producers.com.autodesk.exercise.cdcevent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class CdcFileToKafkaProducer {

    private static final String TOPIC = "user_cdc_topic";
    private static final String INPUT_FILE = "sample_events.json";

    public static void main(String[] args) throws Exception {
    	CdcFileToKafkaProducer cdcFileToKafkaProducer;
    	
    	try {
    		cdcFileToKafkaProducer = new CdcFileToKafkaProducer();
    		cdcFileToKafkaProducer.postCdcEventsToKafka(args);
    	} catch (Exception exc) {
    		exc.printStackTrace();
    	}
    }
    
    public void postCdcEventsToKafka(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper mapper = new ObjectMapper();

        // Discover number of partitions
        int numPartitions = producer.partitionsFor(TOPIC).size();
        int partition;
        String line;
        ProducerRecord<String, String> record;
        JsonNode event, before, after;
        
        try (BufferedReader reader = new BufferedReader(new FileReader(INPUT_FILE))) {
            while ((line = reader.readLine()) != null) {

                event = mapper.readTree(line);
                after = event.get("after");
                before = event.get("before");

                String userId = (after != null && !after.isNull())
                        ? after.get("userId").asText()
                        : before.get("userId").asText();
                
                // assign the Cdc event to a Kafka partition based on userId. 
                // This is because Kafka does not guarantee  a pre-condition to ensure 
                partition = Math.abs(userId.hashCode()) % numPartitions;

                record = new ProducerRecord<>(TOPIC, partition, userId, line);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                        // To do retry with exponential backoff
                    } else {
                        System.out.printf(
                            "Sent userId=%s to partition=%d offset=%d%n",
                            userId,
                            metadata.partition(),
                            metadata.offset()
                        );
                    }
                });
            }
        }

        producer.flush();
        producer.close();
    }    
}
