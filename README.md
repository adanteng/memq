```
CREATE TABLE plsv2.pls_message (
  message_id bigserial NOT NULL,
  message_md5 char(32) NOT NULL DEFAULT '',
  content bytea NOT NULL,
  createtime bigint NOT NULL DEFAULT 0,
  updatetime bigint NOT NULL DEFAULT 0,
  status smallint NOT NULL DEFAULT 0,
  qname varchar(128) NOT NULL DEFAULT ''
);

ALTER TABLE ONLY plsv2.pls_message ADD CONSTRAINT pls_message_pkey PRIMARY KEY (message_id);

CREATE UNIQUE INDEX uq_message_md5 ON pls_message USING btree (message_md5);
```

```
CREATE TABLE plsv2.pls_message_lock (
  lock_id bigserial NOT NULL,
  lock_name varchar(128) NOT NULL DEFAULT '',
  lock_status smallint NOT NULL DEFAULT 0,
  updatetime bigint NOT NULL DEFAULT 0
);

ALTER TABLE ONLY plsv2.pls_message_lock ADD CONSTRAINT pls_message_lock_pkey PRIMARY KEY (lock_id);
```