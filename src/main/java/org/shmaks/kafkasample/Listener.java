package org.shmaks.kafkasample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.serializer.FailedDeserializationInfo;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.function.Function;

@Component
public class Listener {

    private static final Logger logger = LoggerFactory.getLogger(Listener.class.getName());

    private final MessageRepository repo;

    public Listener(MessageRepository repo) {
        this.repo = repo;
    }

    @KafkaListener(id = "kafka-listener", topics = {"${shmaks-kafka.topic}"})
    public void handle(Collection<KafkaMessage> msgs) {

        for (KafkaMessage msg : msgs) {
            if (msg.messages.isEmpty() && msg != KafkaMessage.INVALID) {
                logger.info("received empty message");
            } else if (!msg.messages.isEmpty()){
                try {
                    repo.save(msg.messages);
                    logger.info("received messages: {}", msg.messages);
                } catch (TransientDataAccessException temporary) {
                    throw temporary;
                } catch (DataAccessException e) {
                    logger.info("failed with error for message {}: {}", msg, e);
                }
            }
        }
    }

    public static class IncorrectMessageHandler implements Function<FailedDeserializationInfo, KafkaMessage> {
        @Override
        public KafkaMessage apply(FailedDeserializationInfo info) {
            logger.info("received invalid message: {}", new String(info.getData()));
            return KafkaMessage.INVALID;
        }
    }

}
