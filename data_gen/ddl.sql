CREATE TABLE t_user_stat (
     stat_id MEDIUMINT NOT NULL AUTO_INCREMENT,
     uid INT, 
     stat VARCHAR(30) NOT NULL,
     created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
     PRIMARY KEY (stat_id)
);