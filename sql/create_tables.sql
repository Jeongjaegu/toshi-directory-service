CREATE TYPE HIDDEN_ON_STATE AS ENUM ('none', 'ios', 'android', 'all');

CREATE TABLE IF NOT EXISTS dapps (
    dapp_id BIGINT PRIMARY KEY,
    name VARCHAR NOT NULL,
    url VARCHAR NOT NULL,
    description VARCHAR,
    icon VARCHAR,
    cover VARCHAR,
    special BOOLEAN DEFAULT FALSE,
    hidden_on HIDDEN_ON_STATE DEFAULT 'none',
    created TIMESTAMP WITHOUT TIME ZONE DEFAULT (now() AT TIME ZONE 'utc'),
    updated TIMESTAMP WITHOUT TIME ZONE DEFAULT (now() AT TIME ZONE 'utc'),
    rank INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS categories (
    category_id SERIAL PRIMARY KEY,
    hidden_on HIDDEN_ON_STATE DEFAULT 'none',
    name VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dapp_categories (
    category_id SERIAL,
    dapp_id BIGINT,
    PRIMARY KEY (category_id, dapp_id)
);

UPDATE database_version SET version_number = 3;
