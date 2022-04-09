CREATE DATABASE IF NOT EXISTS db1;
USE db1;
CREATE TABLE IF NOT EXISTS db1.table1(
	id int(11) PRIMARY KEY,
	name varchar(256) ,
	email_id varchar(256) 
);
INSERT INTO table1 VALUES (1,'AJ','aj@gmail.com'),(2,'AJ2','aj2@gmail.com');
CREATE TABLE IF NOT EXISTS db1.table0(
	id int(11) PRIMARY KEY,
	table1_id int(11) ,
	FOREIGN KEY (table1_id) REFERENCES table1(id),
	phone_number varchar(26) NOT NULL
);
INSERT INTO table0 VALUES (1,1,'123456'),(2,1,'2345678'),(3,2,'1234');
