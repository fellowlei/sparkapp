create database spark;
use spark;
create table people(id int,name varchar(20),age int);
insert into people(id,name,age) value(1,"Ganymede",32);
insert into people(id,name,age) value(2, "Lilei", 19);
insert into people(id,name,age) value(3, "Lily", 25);
insert into people(id,name,age) value(4, "Hanmeimei", 25);
insert into people(id,name,age) value(5, "Lucy", 37);
insert into people(id,name,age) value(6, "wcc", 4);

#import to mysql
#mysql -uroot -p -e "source spark.sql"
