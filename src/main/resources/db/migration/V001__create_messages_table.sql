DROP TABLE IF EXISTS messages CASCADE;

CREATE TABLE messages
(
    id      INTEGER NOT NULL CHECK ( id >= 0 ),
    payload text    NOT NULL,

    CONSTRAINT messages_pk PRIMARY KEY (id)
)
