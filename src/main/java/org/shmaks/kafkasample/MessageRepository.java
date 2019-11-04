package org.shmaks.kafkasample;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@Component
public class MessageRepository {

    private final JdbcTemplate jdbc;

    public MessageRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    void save(List<KafkaMessage.Message> msgs) throws DataAccessException {

        jdbc.batchUpdate(
                "insert into messages(id, payload) values(?, ?)",
                new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        ps.setInt(1, msgs.get(i).messageId);
                        ps.setString(2, msgs.get(i).payload);
                    }

                    @Override
                    public int getBatchSize() {
                        return msgs.size();
                    }
                }
        );
    }

}
