use regression_hudi;

DROP TABLE IF EXISTS one_partition_tb;
CREATE TABLE one_partition_tb (
    id INT,
    name string
)
USING HUDI
PARTITIONED BY (part1 INT);
INSERT INTO one_partition_tb PARTITION (part1=2024) VALUES (1, 'Alice');
INSERT INTO one_partition_tb PARTITION (part1=2024) VALUES (2, 'Bob');
INSERT INTO one_partition_tb PARTITION (part1=2024) VALUES (3, 'Charlie');
INSERT INTO one_partition_tb PARTITION (part1=2025) VALUES (4, 'David');
INSERT INTO one_partition_tb PARTITION (part1=2025) VALUES (5, 'Eva');

DROP TABLE IF EXISTS two_partition_tb;
CREATE TABLE two_partition_tb (
    id INT,
    name string
)
USING HUDI
PARTITIONED BY (part1 STRING, part2 int);
INSERT INTO two_partition_tb PARTITION (part1='US', part2=1) VALUES (1, 'Alice');
INSERT INTO two_partition_tb PARTITION (part1='US', part2=1) VALUES (2, 'Bob');
INSERT INTO two_partition_tb PARTITION (part1='US', part2=1) VALUES (3, 'Charlie');
INSERT INTO two_partition_tb PARTITION (part1='US', part2=2) VALUES (4, 'David');
INSERT INTO two_partition_tb PARTITION (part1='US', part2=2) VALUES (5, 'Eva');
INSERT INTO two_partition_tb PARTITION (part1='EU', part2=1) VALUES (6, 'Frank');
INSERT INTO two_partition_tb PARTITION (part1='EU', part2=1) VALUES (7, 'Grace');
INSERT INTO two_partition_tb PARTITION (part1='EU', part2=2) VALUES (8, 'Hannah');
INSERT INTO two_partition_tb PARTITION (part1='EU', part2=2) VALUES (9, 'Ivy');
INSERT INTO two_partition_tb PARTITION (part1='EU', part2=2) VALUES (10, 'Jack');

DROP TABLE IF EXISTS three_partition_tb;
CREATE TABLE three_partition_tb (
    id INT,
    name string
)
USING HUDI
PARTITIONED BY (part1 STRING, part2 INT, part3 STRING);
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2024, part3='Q1') VALUES (1, 'Alice');
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2024, part3='Q1') VALUES (2, 'Bob');
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2024, part3='Q1') VALUES (3, 'Charlie');
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2024, part3='Q2') VALUES (4, 'David');
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2024, part3='Q2') VALUES (5, 'Eva');
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2025, part3='Q1') VALUES (6, 'Frank');
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2025, part3='Q2') VALUES (7, 'Grace');
INSERT INTO three_partition_tb PARTITION (part1='EU', part2=2024, part3='Q1') VALUES (8, 'Hannah');
INSERT INTO three_partition_tb PARTITION (part1='EU', part2=2024, part3='Q1') VALUES (9, 'Ivy');
INSERT INTO three_partition_tb PARTITION (part1='EU', part2=2025, part3='Q2') VALUES (10, 'Jack');
INSERT INTO three_partition_tb PARTITION (part1='EU', part2=2025, part3='Q2') VALUES (11, 'Leo');
INSERT INTO three_partition_tb PARTITION (part1='EU', part2=2025, part3='Q3') VALUES (12, 'Mia');
INSERT INTO three_partition_tb PARTITION (part1='AS', part2=2025, part3='Q1') VALUES (13, 'Nina');
INSERT INTO three_partition_tb PARTITION (part1='AS', part2=2025, part3='Q2') VALUES (14, 'Oscar');
INSERT INTO three_partition_tb PARTITION (part1='AS', part2=2025, part3='Q3') VALUES (15, 'Paul');