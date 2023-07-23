set sql-client.execution.result-mode = tableau;

set execution.checkpointing.interval=30sec;

# flink cdc table -> hudi table
insert into hudi_user_stat
select stat_id, uid, stat, created_at,
       DATE_FORMAT(CURRENT_TIMESTAMP,'yyyyMMdd_HHmm') as `partition`
from user_stat_source;

# test hudi table insert sql
insert into hudi_user_stat
    values (1,1,'#1',CURRENT_TIMESTAMP,'20230718_22_00');