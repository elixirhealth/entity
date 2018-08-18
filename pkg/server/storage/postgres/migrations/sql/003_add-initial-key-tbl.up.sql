CREATE SCHEMA key;

CREATE TABLE key.public_key_detail (
    row_id SERIAL PRIMARY KEY,
    transaction_period TSTZRANGE NOT NULL DEFAULT tstzrange(NOW(), 'infinity', '[)'),
    public_key bytea NOT NULL,
    key_type VARCHAR NOT NULL,
    entity_id VARCHAR NOT NULL
);

CREATE UNIQUE INDEX public_key_detail_public_key ON key.public_key_detail (public_key);
CREATE INDEX public_key_detail_entity_id_key_type ON key.public_key_detail (entity_id, key_type);
