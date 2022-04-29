use TestDatabase
go

create table dbo.LocationsGeo
(
	Id int not null identity(1,1),
	Location geography not null,
	constraint PK_LocationsGeo primary key clustered(Id)
);

-- Approximate quare around Serbia
-- 161202 records
;with Latitudes(Lat)
as
(
	select convert(float,42.0)
	union all
	select convert(float,Lat + 0.01)
	from Latitudes
	where Lat < 46
)
,Longitudes(Lon)
as
(
	select convert(float,19.0)
	union all
	select Lon + 0.01
	from Longitudes
	where Lon < 23
)
insert into dbo.LocationsGeo(Location)
-- SRID 4326 in Point constructor is referring to World Geodetic System 1984 
select geography::Point(Lat, Lon, 4326)
from Latitudes cross join Longitudes
option (maxrecursion 0);
go

-- Elfak
declare @g geography;
set @g = geography::Point(43.3267515, 21.8933164, 4326);
-- 5km within
select Id
from dbo.LocationsGeo
where Location.STDistance(@g) < 5000;
go

-- Create Index
create spatial index Idx_LocationsGeo_Spatial
on dbo.LocationsGeo(Location);
go

-- Retry
declare @g geography;
set @g = geography::Point(43.3267515, 21.8933164, 4326);
select Id
from dbo.LocationsGeo
where Location.STDistance(@g) < 5000;