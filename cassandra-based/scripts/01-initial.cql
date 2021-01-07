-- ############################################ --
use system_schema;
select * from system_schema.keyspaces;
SELECT cluster_name, listen_address FROM system.local;