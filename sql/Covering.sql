use TestDatabase
go

create table dbo.Customers
(
	CustomerId int not null identity(1,1),
	FirstName nvarchar(64) not null,
	LastName nvarchar(128) not null,
	Birthday datetime not null,
	ShortBio char(200) null
)

create unique clustered index IDX_Customers_CustomerId
on dbo.Customers(CustomerId)

;with FirstNames(FirstName)
as
(
select Names.Name
from ( values('Andrew'),('Andy'),('Anton'),('Ashley'),('Boris'),('Brian'),
('Cristopher'),('Cathy')
, ('Daniel'),('Donny'),('Edward'),('Eddy'),('Emy'),('Frank'),('George'),
('Harry'),('Henry')
, ('Ida'),('John'),('Jimmy'),('Jenny'),('Jack'),('Kathy'),('Kim'),('Larry'),
('Mary'),('Max')
, ('Nancy'),('Olivia'),('Olga'),('Peter'),('Patrick'),('Robert'),('Ron'),
('Steve'),('Shawn')
,('Tom'),('Timothy'),('Uri'),('Vincent') ) Names(Name)
)
,LastNames(LastName)
as
(
select Names.Name
from ( values('Smith'),('Johnson'),('Williams'),('Jones'),('Brown'),('Davis'),('Miller')
,('Wilson'), ('Moore'),('Taylor'),('Anderson'),('Jackson'),('White'),('Harris') )
Names(Name)
)
insert into dbo.Customers(LastName, FirstName, Birthday)
select LastName, FirstName, getdate() from FirstNames cross join LastNames
go 50

create nonclustered index IDX_Customers_LastName_FirstName
on dbo.Customers(LastName, FirstName)

------------------

-- Queries

select CustomerId, LastName, FirstName, Birthday
from dbo.Customers
where LastName = 'Anderson'

select CustomerId, LastName, FirstName, Birthday
from dbo.Customers with (Index=IDX_Customers_LastName_FirstName_BirthdayIncluded)
where LastName = 'Anderson'

------------------

create nonclustered index IDX_Customers_LastName_FirstName_BirthdayIncluded
on dbo.Customers(LastName, FirstName)
include(Birthday)

------------------

select CustomerId, LastName, FirstName, Birthday
from dbo.Customers
where LastName = 'Anderson'