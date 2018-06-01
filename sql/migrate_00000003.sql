CREATE TYPE HIDDEN_ON_STATE AS ENUM ('none', 'ios', 'android', 'all');

ALTER TABLE dapps ADD COLUMN hidden_on HIDDEN_ON_STATE DEFAULT 'none';
ALTER TABLE categories ADD COLUMN hidden_on HIDDEN_ON_STATE DEFAULT 'none';
