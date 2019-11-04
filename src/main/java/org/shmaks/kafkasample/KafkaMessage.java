package org.shmaks.kafkasample;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class KafkaMessage {

    public static final KafkaMessage INVALID = new KafkaMessage(Collections.emptyList()) {
        public final boolean invalid = true;
    };

    public final List<Message> messages;

    @JsonCreator
    public KafkaMessage(@JsonProperty("messages") List<Message> messages) {
        if (messages == null) {
            throw new NullPointerException("'messages' is null");
        }
        this.messages = Collections.unmodifiableList(messages);
    }

    @Override
    public String toString() {
        return "KafkaMessage{" +
                "messages=" + messages +
                '}';
    }

    public static class Message {
        public final int messageId;
        public final String payload;

        @JsonCreator
        public Message(@JsonProperty("messageId") Integer messageId,
                       @JsonProperty("payload") String payload) {
            if (messageId == null || messageId < 0) {
                throw new IllegalArgumentException("Incorrect 'messageId'");
            }
            if (payload == null) {
                throw new NullPointerException("'payload' is null");
            }
            this.messageId = messageId;
            this.payload = payload;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Message message = (Message) o;
            return messageId == message.messageId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(messageId);
        }

        @Override
        public String toString() {
            return "Message{" +
                    "messageId=" + messageId +
                    ", payload='" + payload + '\'' +
                    '}';
        }
    }
}
