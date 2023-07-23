CREATE TABLE user_stat_source (
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    table_name STRING METADATA  FROM 'table_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    stat_id int NOT NULL,
    uid int,
    stat VARCHAR(30),
    created_at TIMESTAMP(0),
    PRIMARY KEY (`stat_id`) NOT ENFORCED
)
WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '00.00.00.00',
    'port' = '3306',
    'username' = 'db',
    'password' = 'password',
    'database-name' = 'meitu_db',
    'table-name' = 't_user_stat'
);