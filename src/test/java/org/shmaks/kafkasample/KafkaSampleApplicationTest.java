package org.shmaks.kafkasample;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

@TestPropertySource(locations = { "classpath:config-test.properties", "classpath:/application-test.yml" })
@SpringBootTest
@EmbeddedKafka(brokerProperties = {
        "listeners=PLAINTEXT://${shmaks-kafka.broker}"
})
@ContextConfiguration(initializers = {KafkaSampleApplicationTest.Initializer.class})
@Tag("integration-test")
class KafkaSampleApplicationTest {

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private JdbcTemplate jdbc;

    @ClassRule
    public static PostgreSQLContainer postgreSQLContainer = PgTestContainer.getInstance();

    @Value("${shmaks-kafka.topic}")
    private String topic;

    @Test
    void contextLoads() {
    }

    @BeforeEach
    void setUp() {
        jdbc.execute("truncate table messages");
    }

    @Test
	void receiveCorrectBatch() throws Exception {
        for (int i = 0; i < 100; i++) {
            KafkaMessage msg1 = new KafkaMessage(Arrays.asList(
                    new KafkaMessage.Message(i, "TEST0"),
                    new KafkaMessage.Message(100 + i, "TEST1")
            ));

            KafkaMessage msg2 = new KafkaMessage(Arrays.asList(
                    new KafkaMessage.Message(200 + i, "TEST2"),
                    new KafkaMessage.Message(300 + i, "TEST3"),
                    new KafkaMessage.Message(500 + i, "TEST5")
            ));


            kafkaTemplate.send(topic, mapper.writeValueAsString(msg1));
            kafkaTemplate.send(topic, mapper.writeValueAsString(msg2));
        }

        sleep();

        assertThat(jdbc.queryForObject("select count(*) from messages", Integer.class)).isEqualTo(500);
	}

    @Test
    void receiveIncorrectBatch() throws Exception {

        for (int i = 0; i < 30; i++) {
            kafkaTemplate.send(topic, "smth" + i);
            kafkaTemplate.send(topic, "{\"messages\": null}");
            kafkaTemplate.send(topic, "{\"messages\": [{\"messageId\": -1, \"payload\": \"incorrect\"}, {\"messageId\": 1, \"payload\": \"correct\"}]}");
            kafkaTemplate.send(topic, "{\"messages\": [{\"messageId\": 1, \"payload\": \"correct\"}, {\"messageId\": 123}, {\"messageId\": 0, \"payload\": \"\"}]}");
            kafkaTemplate.send(topic, "{\"messages\": [{\"messageId\": 1, \"payload\": null}]}");
            kafkaTemplate.send(topic, "{\"messages\": [{\"payload\": \"incorrect\"}, { \"payload\": \"incorrect\"}]}");
        }

        sleep();

        assertThat(jdbc.queryForObject("select count(*) from messages", Integer.class)).isEqualTo(0);
    }

    @Test
    void receiveCorrectAndIncorrectBatch() throws Exception {
        for (int i = 0; i < 30; i++) {
            kafkaTemplate.send(topic, "smth" + i);

            KafkaMessage msg = new KafkaMessage(Arrays.asList(
                    new KafkaMessage.Message(i, "TEST0"),
                    new KafkaMessage.Message(100 + i, "TEST1")
            ));
            kafkaTemplate.send(topic, mapper.writeValueAsString(msg));
        }

        sleep();

        assertThat(jdbc.queryForObject("select count(*) from messages", Integer.class)).isEqualTo(60);
    }

    @Test
    void receiveDuplicates() throws Exception {
        for (int i = 0; i < 30; i++) {
            KafkaMessage msg = new KafkaMessage(Arrays.asList(
                    new KafkaMessage.Message(i, "0"),
                    new KafkaMessage.Message(i + 15, "1")
            ));
            kafkaTemplate.send(topic, mapper.writeValueAsString(msg));
        }

        sleep();

        assertThat(jdbc.queryForObject("select count(*) from messages", Integer.class)).isEqualTo(30);
        assertThat(jdbc.queryForObject("select count(*) from messages where payload = ?", new Object[]{"0"}, Integer.class)).isEqualTo(15);
        assertThat(jdbc.queryForObject("select count(*) from messages where payload = ?", new Object[]{"1"}, Integer.class)).isEqualTo(15);
    }

    @Test
    void receiveCorrectWithTemporaryDbError() throws Exception {
        postgreSQLContainer.stop();

        for (int i = 0; i < 30; i++) {
            KafkaMessage msg = new KafkaMessage(Collections.singletonList(
                    new KafkaMessage.Message(i, "0")
            ));
            kafkaTemplate.send(topic, mapper.writeValueAsString(msg));
        }

        sleep();

        postgreSQLContainer.start();

        sleep();

        assertThat(jdbc.queryForObject("select count(*) from messages", Integer.class)).isEqualTo(30);
    }

    private void sleep() throws InterruptedException {
        Thread.sleep(2_000);
    }

    static class Initializer
            implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            postgreSQLContainer.start();
            TestPropertyValues.of(
                    "shmaks-kafka.pg.url=" + postgreSQLContainer.getJdbcUrl(),
                    "shmaks-kafka.pg.username=" + postgreSQLContainer.getUsername(),
                    "shmaks-kafka.pg.password=" + postgreSQLContainer.getPassword()
            ).applyTo(configurableApplicationContext.getEnvironment());
        }
    }
}
