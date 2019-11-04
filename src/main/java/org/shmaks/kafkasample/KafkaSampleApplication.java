package org.shmaks.kafkasample;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.BatchLoggingErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

@SpringBootApplication
@EnableKafka
public class KafkaSampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaSampleApplication.class, args);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
			ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
			ConsumerFactory<Object, Object> kafkaConsumerFactory) {
		ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		configurer.configure(factory, kafkaConsumerFactory);
		factory.setBatchErrorHandler(new BatchLoggingErrorHandler());
//		factory.setErrorHandler(new SeekToCurrentErrorHandler(
//				new DeadLetterPublishingRecoverer(template), new FixedBackOff(0L, 2)));
		return factory;
	}

	@Bean
	public ConsumerFactory<Object, Object> kafkaConsumerFactory(KafkaProperties kafkaProperties) {
		Map<String, Object> props = kafkaProperties.buildConsumerProperties();
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer2.class);
		props.put(ErrorHandlingDeserializer2.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
		props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, KafkaMessage.class.getName());
		props.put(ErrorHandlingDeserializer2.VALUE_FUNCTION, Listener.IncorrectMessageHandler.class);
		props.put(JsonDeserializer.TRUSTED_PACKAGES, KafkaMessage.class.getPackage().getName());
		return new DefaultKafkaConsumerFactory<>(props);
	}

	@Bean
	public NewTopic topic(@Value("${shmaks-kafka.topic}") String name,
						  @Value("${shmaks-kafka.topic-partitions}") int partitions) {
		return new NewTopic(name, partitions, (short) 1);
	}

}
