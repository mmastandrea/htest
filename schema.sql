drop table if exists servicetypes; 
create table servicetypes (
	id integer primary key autoincrement,
	servicetype text not null,
	policyname text not null
);

insert into servicetypes (servicetype,policyname) values
('GOLD', 'BA_HIGH');
insert into servicetypes (servicetype,policyname) values
('SILVER', 'BA_MEDIUM');
insert into servicetypes (servicetype,policyname) values
('BRONZE', 'BA_LOW');
	

drop table if exists activations; 
create table activations(
	id integer primary key autoincrement,
	clientip text not null,
	servicetype text not null,
	duration integer not null
);

drop table if exists rules;
create table rules(
	id integer primary key autoincrement,
	serialnumber text not null,
	servicetype text not null,
	startip text not null,
	endip text not null
);

insert into rules (serialnumber,servicetype,startip,endip) values
('2895000000099','GOLD', '10.34.3.1','10.34.3.255');
insert into rules (serialnumber,servicetype,startip,endip) values
('2895000000090','GOLD', '10.34.5.1','10.34.5.255');
insert into rules (serialnumber,servicetype,startip,endip) values
('2895000000089','GOLD', '10.34.6.1','10.34.6.255');
	
