set noexec off
go

use master
go


if exists
(
	select * from sys.databases where name = 'InMemory'
)
begin
	raiserror('Database InMemory already exists',16,1)
	set noexec on
end
go


declare
	@dataPath nvarchar(512) = convert(nvarchar(512),serverproperty('InstanceDefaultDataPath'))
	,@logPath nvarchar(512) = convert(nvarchar(512),serverproperty('InstanceDefaultLogPath'))

-- HK Data = 'Hekaton', project codename
declare
	@HKPath nvarchar(512) = @dataPath + 'InMemory_HKData'
	
declare
	@SQL nvarchar(max)

select @SQL = 
N'create database [InMemory] on 
primary (name=N''InMemory'', filename=N''' + @dataPath + N'InMemory.mdf'', size=100MB, filegrowth = 50MB),
filegroup [HKData] contains memory_optimized_data (name=N''InMemory_HekatonData'', filename=N''' + @HKPath + N''')
log on (name=N''InMemory_log'', filename=N''' + @logPath + N'InMemory.ldf'', size=100MB, filegrowth = 100MB);
alter database [InMemory] set recovery simple;'

raiserror('Creating database InMemory',0,1) with nowait
raiserror('Data Path: %s',0,1,@dataPath) with nowait
raiserror('Log Path: %s',0,1,@logPath) with nowait
raiserror('Hekaton Folder: %s',0,1,@HKPath) with nowait
raiserror('Statement:',0,1) with nowait
raiserror(@sql,0,1) with nowait

exec sp_executesql @sql
go

use InMemory
go

if exists(select * from sys.tables t join sys.schemas s on t.schema_id = s.schema_id where s.name = 'dbo' and t.name = 'CustomersOnDisk') drop table dbo.CustomersOnDisk;
if exists(select * from sys.tables t join sys.schemas s on t.schema_id = s.schema_id where s.name = 'dbo' and t.name = 'CustomersMemoryOptimized') drop table dbo.CustomersMemoryOptimized;
go

create table dbo.CustomersOnDisk
(
	CustomerId int not null identity(1,1),
	FirstName varchar(64),
	LastName varchar(64),
	Placeholder char(100) null,

	constraint PK_CustomersOnDisk
	primary key clustered(CustomerId)
);

create nonclustered index IDX_CustomersOnDisk_LastName_FirstName
on dbo.CustomersOnDisk(LastName, FirstName);
go

create table dbo.CustomersMemoryOptimized
(
	CustomerId int not null identity(1,1)
		constraint PK_CustomersMemoryOptimized
		primary key nonclustered 
		hash with (bucket_count = 30000),
	FirstName varchar(64),
	LastName varchar(64),
	Placeholder char(100) null,

	index IDX_CustomersMemoryOptimized_LastName_FirstName
	nonclustered hash(LastName, FirstName)
	with (bucket_count = 1024),
)
with (memory_optimized = on, durability = schema_only)
go

-- Data generation, first 50 times cross-product onto disk then copy to memory-optimized
;with N1(C) as (select 0 union all select 0) -- 2 rows
,N2(C) as (select 0 from N1 as T1 cross join N1 as T2) -- 4 rows
,N3(C) as (select 0 from N2 as T1 cross join N2 as T2) -- 16 rows
,N4(C) as (select 0 from N3 as T1 cross join N2 as T2) -- 64 rows
,IDs(ID) as (select ROW_NUMBER() over (order by (select null)) from N4)
,FirstNames(FirstName)
as
(
	select Names.Name
	from 
	(
		values('Andrew'),('Andy'),('Anton'),('Ashley'),('Boris'),
		('Brian'),('Cristopher'),('Cathy'),('Daniel'),('Donny'),
		('Edward'),('Eddy'),('Emy'),('Frank'),('George'),('Harry'),
		('Henry'),('Ida'),('John'),('Jimmy'),('Jenny'),('Jack'),
		('Kathy'),('Kim'),('Larry'),('Mary'),('Max'),('Nancy'),
		('Olivia'),('Paul'),('Peter'),('Patrick'),('Robert'),
		('Ron'),('Steve'),('Shawn'),('Tom'),('Timothy'),
		('Uri'),('Vincent')
	) Names(Name)
)
,LastNames(LastName)
as
(
	select Names.Name
	from 
	(
		values('Smith'),('Johnson'),('Williams'),('Jones'),('Brown'),
			('Davis'),('Miller'),('Wilson'),('Moore'),('Taylor'),
			('Anderson'),('Jackson'),('White'),('Harris')
	) Names(Name)
)
insert into dbo.CustomersOnDisk(LastName, FirstName)
	select LastName, FirstName
	from FirstNames cross join LastNames cross join IDs
go 50

insert into dbo.CustomersMemoryOptimized(LastName, FirstName)
	select LastName, FirstName
	from dbo.CustomersOnDisk;
go

