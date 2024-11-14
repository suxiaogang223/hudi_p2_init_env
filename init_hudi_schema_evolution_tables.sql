-- 测试 Schema Evolution 功能的 Hudi 表构造案例
use regression_hudi;

-- 简单类型列的测试

-- 1. 添加新列（Adding Columns）
DROP TABLE IF EXISTS adding_simple_columns_table;
CREATE TABLE adding_simple_columns_table (
    id STRING,
    name STRING
) USING HUDI OPTIONS (
    primaryKey = 'id',
    preCombineField = 'id'
);

-- 导入初始数据
INSERT INTO adding_simple_columns_table VALUES 
('1', 'Alice'),
('2', 'Bob'),
('3', 'Cathy');

-- 添加新列 age
ALTER TABLE adding_simple_columns_table ADD COLUMNS (age INT);

-- 导入数据（主键不重复）
INSERT INTO adding_simple_columns_table VALUES 
('4', 'David', 25),
('5', 'Eva', 30),
('6', 'Frank', 28);

-- -- 2. 修改列位置（Altering Column Position）
-- DROP TABLE IF EXISTS altering_simple_columns_table;
-- CREATE TABLE altering_simple_columns_table (
--     id STRING,
--     name STRING,
--     age INT
-- ) USING HUDI OPTIONS (
--     primaryKey = 'id',
--     preCombineField = 'id'
-- );

-- -- 导入初始数据
-- INSERT INTO altering_simple_columns_table VALUES 
-- ('1', 'Alice', 25),
-- ('2', 'Bob', 30),
-- ('3', 'Cathy', 28);

-- -- 修改列位置，将 age 和 id 位置互换
-- ALTER TABLE altering_simple_columns_table CHANGE COLUMN age age INT AFTER id;

-- -- 导入数据（主键不重复）
-- INSERT INTO altering_simple_columns_table VALUES 
-- ('4', 'David', 26),
-- ('5', 'Eva', 31),
-- ('6', 'Frank', 29);

-- -- 3. 删除列（Deleting Columns）
-- DROP TABLE IF EXISTS deleting_simple_columns_table;
-- CREATE TABLE deleting_simple_columns_table (
--     id STRING,
--     name STRING,
--     age INT
-- ) USING HUDI OPTIONS (
--     primaryKey = 'id',
--     preCombineField = 'id'
-- );

-- -- 导入初始数据
-- INSERT INTO deleting_simple_columns_table VALUES 
-- ('1', 'Alice', 25),
-- ('2', 'Bob', 30),
-- ('3', 'Cathy', 28);

-- -- 删除列 age
-- ALTER TABLE deleting_simple_columns_table DROP COLUMN age;

-- -- 导入数据（主键不重复）
-- INSERT INTO deleting_simple_columns_table VALUES 
-- ('4', 'David'),
-- ('5', 'Eva'),
-- ('6', 'Frank');

-- 4. 重命名列（Renaming Columns）
DROP TABLE IF EXISTS renaming_simple_columns_table;
CREATE TABLE renaming_simple_columns_table (
    id STRING,
    name STRING
) USING HUDI OPTIONS (
    primaryKey = 'id',
    preCombineField = 'id'
);

-- 导入初始数据
INSERT INTO renaming_simple_columns_table VALUES 
('1', 'Alice'),
('2', 'Bob'),
('3', 'Cathy');

-- 重命名列 name 为 full_name
ALTER TABLE renaming_simple_columns_table RENAME COLUMN name TO full_name;

-- 导入数据（主键不重复）
INSERT INTO renaming_simple_columns_table VALUES 
('4', 'David'),
('5', 'Eva'),
('6', 'Frank');

-- 复杂类型列的测试

-- 1. 添加复杂类型列（Adding Columns）
DROP TABLE IF EXISTS adding_complex_columns_table;
CREATE TABLE adding_complex_columns_table (
    id STRING,
    name STRING,
    info STRUCT<age: INT, address: STRING>
) USING HUDI OPTIONS (
    primaryKey = 'id',
    preCombineField = 'id'
);

-- 导入初始数据
INSERT INTO adding_complex_columns_table VALUES 
('1', 'Alice', named_struct('age', 25, 'address', 'Guangzhou')),
('2', 'Bob', named_struct('age', 30, 'address', 'Shanghai')),
('3', 'Cathy', named_struct('age', 28, 'address', 'Beijing'));

-- 添加新列 email 到 info 结构体中
ALTER TABLE adding_complex_columns_table ALTER COLUMN info ADD COLUMNS (email STRING);

-- 导入数据（主键不重复）
INSERT INTO adding_complex_columns_table VALUES 
('4', 'David', named_struct('age', 25, 'address', 'Shenzhen', 'email', 'david@example.com')),
('5', 'Eva', named_struct('age', 30, 'address', 'Chengdu', 'email', 'eva@example.com')),
('6', 'Frank', named_struct('age', 28, 'address', 'Wuhan', 'email', 'frank@example.com'));

-- -- 2. 修改复杂类型结构体中的列位置（Altering Column Position）
-- DROP TABLE IF EXISTS altering_complex_columns_table;
-- CREATE TABLE altering_complex_columns_table (
--     id STRING,
--     name STRING,
--     info STRUCT<age: INT, address: STRING>
-- ) USING HUDI OPTIONS (
--     primaryKey = 'id',
--     preCombineField = 'id'
-- );

-- -- 导入初始数据
-- INSERT INTO altering_complex_columns_table VALUES 
-- ('1', 'Alice', named_struct('age', 25, 'address', 'Guangzhou')),
-- ('2', 'Bob', named_struct('age', 30, 'address', 'Shanghai')),
-- ('3', 'Cathy', named_struct('age', 28, 'address', 'Beijing'));

-- -- 修改 info 中列的位置，将 age 移动到 address 后面
-- ALTER TABLE altering_complex_columns_table ALTER COLUMN info CHANGE COLUMN age age INT AFTER address;

-- -- 导入数据（主键不重复）
-- INSERT INTO altering_complex_columns_table VALUES 
-- ('4', 'David', named_struct('address', 'Shenzhen', 'age', 26)),
-- ('5', 'Eva', named_struct('address', 'Chengdu', 'age', 31)),
-- ('6', 'Frank', named_struct('address', 'Wuhan', 'age', 29));

-- 3. 删除复杂类型结构体中的列（Deleting Columns）
DROP TABLE IF EXISTS deleting_complex_columns_table;
CREATE TABLE deleting_complex_columns_table (
    id STRING,
    name STRING,
    info STRUCT<age: INT, address: STRING, email: STRING>
) USING HUDI OPTIONS (
    primaryKey = 'id',
    preCombineField = 'id'
);

-- 导入初始数据
INSERT INTO deleting_complex_columns_table VALUES 
('1', 'Alice', named_struct('age', 25, 'address', 'Guangzhou', 'email', 'alice@example.com')),
('2', 'Bob', named_struct('age', 30, 'address', 'Shanghai', 'email', 'bob@example.com')),
('3', 'Cathy', named_struct('age', 28, 'address', 'Beijing', 'email', 'cathy@example.com'));

-- 删除 info.email
ALTER TABLE deleting_complex_columns_table ALTER COLUMN info DROP COLUMN email;

-- 导入数据（主键不重复）
INSERT INTO deleting_complex_columns_table VALUES 
('4', 'David', named_struct('age', 25, 'address', 'Shenzhen')),
('5', 'Eva', named_struct('age', 30, 'address', 'Chengdu')),
('6', 'Frank', named_struct('age', 28, 'address', 'Wuhan'));

-- 4. 重命名复杂类型结构体中的列（Renaming Columns）
DROP TABLE IF EXISTS renaming_complex_columns_table;
CREATE TABLE renaming_complex_columns_table (
    id STRING,
    name STRING,
    info STRUCT<age: INT, address: STRING>
) USING HUDI OPTIONS (
    primaryKey = 'id',
    preCombineField = 'id'
);

-- 导入初始数据
INSERT INTO renaming_complex_columns_table VALUES 
('1', 'Alice', named_struct('age', 25, 'address', 'Guangzhou')),
('2', 'Bob', named_struct('age', 30, 'address', 'Shanghai')),
('3', 'Cathy', named_struct('age', 28, 'address', 'Beijing'));

-- 重命名 info.address 为 info.location
ALTER TABLE renaming_complex_columns_table ALTER COLUMN info RENAME COLUMN address TO location;

-- 导入数据（主键不重复）
INSERT INTO renaming_complex_columns_table VALUES 
('4', 'David', named_struct('age', 25, 'location', 'Shenzhen')),
('5', 'Eva', named_struct('age', 30, 'location', 'Chengdu')),
('6', 'Frank', named_struct('age', 28, 'location', 'Wuhan'));
