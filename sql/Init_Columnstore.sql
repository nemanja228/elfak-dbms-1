set noexec off
go

use master
go

if exists
(
	select * from sys.databases where name = 'Columnstore'
)
begin
	raiserror('Database Columnstore already exists',16,1)
	set noexec on
end
go


declare
	@dataPath nvarchar(512)
	,@logPath nvarchar(512) 

exec master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'DefaultData', @dataPath output
exec master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'DefaultLog', @logPath output

-- Creating database in the same folder with master
if @dataPath is null
	select @dataPath = substring(physical_name, 1, len(physical_name) - charindex('\', reverse(physical_name))) + '\'
	from master.sys.database_files 
	where file_id = 1

if @logPath is null
	select @logPath = substring(physical_name, 1, len(physical_name) - charindex('\', reverse(physical_name))) + '\'
	from master.sys.database_files 
	where file_id = 2
	
if @dataPath is null or @logPath is null
begin
	raiserror('Cannot obtain path for data and/or log file',16,1)
	set noexec on
end

if right(@dataPath, 1) <> '\'
	select @dataPath = @dataPath + '\'
if right(@logPath, 1) <> '\'
	select @logPath = @logPath + '\'
	
declare
	@SQL nvarchar(max)

select @SQL = 
	replace
	(
		replace(
N'create database [Columnstore]
on primary (name=N''Columnstore'', filename=N''%DATA%Data.mdf'', size=10MB, filegrowth = 10MB)
log on (name=N''Columnstore_log'', filename=N''%LOG%Columnstore.ldf'', size=256MB, filegrowth = 256MB);
alter database [Columnstore] set recovery simple;'
			,'%DATA%',@dataPath
		),'%LOG%',@logPath
	)

raiserror('Creating database Columnstore',0,1) with nowait
raiserror('Data Path: %s',0,1,@dataPath) with nowait
raiserror('Log Path: %s',0,1,@logPath) with nowait
raiserror('Statement:',0,1) with nowait
raiserror(@sql,0,1) with nowait

exec sp_executesql @sql
go

use Columnstore
go

-- Create Dim tables

create table dbo.DimBranches
(
	BranchId int not null primary key,
	BranchNumber nvarchar(32) not null,
	BranchCity nvarchar(32) not null,
	BranchRegion nvarchar(32) not null,
	BranchCountry nvarchar(32) not null
);

create table dbo.DimArticles
(
	ArticleId int not null primary key,
	ArticleCode nvarchar(32) not null,
	ArticleCategory nvarchar(32) not null
);

create table dbo.DimDates
(
	DateId int not null primary key,
	ADate date not null,
	ADay tinyint not null,
	AMonth tinyint not null,
	AnYear smallint not null,
	AQuarter tinyint not null,
	ADayOfWeek tinyint not null
);

-- Create Fact table
create table dbo.FactSales
(
	DateId int not null
	foreign key references dbo.DimDates(DateId),
	ArticleId int not null
	foreign key references dbo.DimArticles(ArticleId),
	BranchId int not null
	foreign key references dbo.DimBranches(BranchId),
	OrderId int not null,
	Quantity decimal(9,3) not null,
	UnitPrice money not null,
	Amount money not null,
	DiscountPcnt decimal (6,3) not null,
	DiscountAmt money not null,
	TaxAmt money not null,
	constraint PK_FactSales primary key (DateId, ArticleId, BranchId, OrderId)
	with (data_compression = page)
);

-- Fill DimDates
;with N1(C) as (select 0 union all select 0) -- 2 rows
,N2(C) as (select 0 from N1 as T1 cross join N1 as T2) -- 4 rows
,N3(C) as (select 0 from N2 as T1 cross join N2 as T2) -- 16 rows
,N4(C) as (select 0 from N3 as T1 cross join N3 as T2) -- 256 rows
,N5(C) as (select 0 from N2 as T1 cross join N4 as T2) -- 1,024 rows
,IDs(ID) as (select row_number() over (order by (select null)) from N5)
,Dates(DateId, ADate)
as
(
	select ID, dateadd(day,ID,'2014-12-31')
	from IDs
	where ID <= 727
)
insert into dbo.DimDates(DateId, ADate, ADay, AMonth, AnYear, AQuarter, ADayOfWeek)
select DateID, ADate, Day(ADate), Month(ADate), Year(ADate), datepart(qq,ADate),
datepart(dw,ADate)
from Dates;

-- Fill DimBranches
;with N1(C) as (select 0 union all select 0) -- 2 rows
,N2(C) as (select 0 from N1 as T1 cross join N1 as T2) -- 4 rows
,N3(C) as (select 0 from N2 as T1 cross join N2 as T2) -- 16 rows
,IDs(ID) as (select row_number() over (order by (select null)) from N3)
insert into dbo.DimBranches(BranchId, BranchNumber, BranchCity, BranchRegion, BranchCountry)
select ID, convert(nvarchar(32),ID), 'City', 'Region', 'Country' from IDs where ID <= 13;

-- Fill DimArticles
;with N1(C) as (select 0 union all select 0) -- 2 rows
,N2(C) as (select 0 from N1 as T1 cross join N1 as T2) -- 4 rows
,N3(C) as (select 0 from N2 as T1 cross join N2 as T2) -- 16 rows
,N4(C) as (select 0 from N3 as T1 cross join N3 as T2) -- 256 rows
,N5(C) as (select 0 from N4 as T1 cross join N2 as T2) -- 1,024 rows
,IDs(ID) as (select row_number() over (order by (select null)) from N5)
insert into dbo.DimArticles(ArticleId, ArticleCode, ArticleCategory)
select ID, convert(nvarchar(32),ID), 'Category ' + convert(nvarchar(32),ID % 51)
from IDs
where ID <= 1021;

-- Fill FactSales
;with N1(C) as (select 0 union all select 0) -- 2 rows
,N2(C) as (select 0 from N1 as T1 cross join N1 as T2) -- 4 rows
,N3(C) as (select 0 from N2 as T1 cross join N2 as T2) -- 16 rows
,N4(C) as (select 0 from N3 as T1 cross join N3 as T2) -- 256 rows
,N5(C) as (select 0 from N4 as T1 cross join N4 as T2) -- 65,536 rows
,N6(C) as (select 0 from N5 as T1 cross join N4 as T2) -- 16,777,216 rows
,IDs(ID) as (select row_number() over (order by (select null)) from N6)
insert into dbo.FactSales(DateId, ArticleId, BranchId, OrderId, Quantity, UnitPrice, Amount
,DiscountPcnt, DiscountAmt, TaxAmt)
select ID % 727 + 1, ID % 1021 + 1, ID % 13 + 1, ID, ID % 51 + 1, ID % 25 + 0.99
,(ID % 51 + 1) * (ID % 25 + 0.99), 0, 0, (ID % 25 + 0.99) * (ID % 10) * 0.01
from IDs;

-- Create nonclustered index for comparison based on workload (test query)
create nonclustered columnstore index IDX_FactSales_ColumnStore
on dbo.FactSales(DateId, ArticleId, BranchId, Quantity, UnitPrice, Amount);