CREATE TABLE runners (
  id             uuid PRIMARY KEY,
  name           text        NOT NULL,
  labels         text[]      NOT NULL DEFAULT '{}',
  mode           text        NOT NULL,               -- host|docker|k8s
  capacity       int         NOT NULL DEFAULT 1,
  version        text        NOT NULL DEFAULT '',
  status         text        NOT NULL DEFAULT 'offline', -- online|draining|offline
  last_heartbeat timestamptz,
  created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX runners_status_idx ON runners (status);
