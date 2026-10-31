CREATE TABLE IF NOT EXISTS snippets (
  paste_key varchar(8) PRIMARY KEY,
  created_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL,
  content text NOT NULL
);

CREATE INDEX IF NOT EXISTS snippets_expires_idx ON snippets(expires_at);

CREATE TABLE IF NOT EXISTS idempotency_keys (
  idem_key varchar(200) PRIMARY KEY,
  fingerprint varchar(64) NOT NULL,
  paste_key varchar(8),
  created_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS idem_created_idx ON idempotency_keys(created_at);
