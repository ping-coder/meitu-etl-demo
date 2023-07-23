create table hudi_user_stat(
    stat_id int NOT NULL,
    uid int,
    stat VARCHAR(30),
    created_at TIMESTAMP(0),
    `partition` VARCHAR(20),
    PRIMARY KEY (`stat_id`) NOT ENFORCED
)
PARTITIONED BY (`partition`)
WITH (
    'connector' = 'hudi',
    'path' = 'gs://peace-demo/meitu_raw',
    'table.type' = 'COPY_ON_WRITE',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '1'
);