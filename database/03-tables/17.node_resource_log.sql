CREATE TABLE node_resource_log
(
  node_name character varying NOT NULL,
  cpu_user SMALLINT NOT NULL,
  cpu_system SMALLINT NOT NULL,
  mem_total_kb INT NOT NULL,
  mem_used_kb INT NOT NULL,
  swap_total_kb INT NOT NULL,
  swap_used_kb INT NOT NULL,
  load_avg_1m INT NOT NULL,
  load_avg_5m INT NOT NULL,
  load_avg_15m INT NOT NULL,
  disk_total_bytes BIGINT NOT NULL,
  disk_used_bytes BIGINT NOT NULL,
  "timestamp" TIMESTAMP WITH TIME ZONE NOT NULL
);
