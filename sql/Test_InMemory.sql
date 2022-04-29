use InMemory
go

-- Match Index
select CustomerId, FirstName, LastName
from dbo.CustomersOnDisk
where FirstName = 'Paul' and LastName = 'White';

select CustomerId, FirstName, LastName
from dbo.CustomersMemoryOptimized
where FirstName = 'Paul' and LastName = 'White';
go

-- No Match
select CustomerId, FirstName, LastName
from dbo.CustomersOnDisk
where LastName = 'White';

select CustomerId, FirstName, LastName
from dbo.CustomersMemoryOptimized
where LastName = 'White';
go

-- Add missing index on in-memory table
alter table [dbo].[CustomersMemoryOptimized]
add index IDX_CustomersMemoryOptimized_LastName
nonclustered ([LastName])

-- Retry
select CustomerId, FirstName, LastName
from dbo.CustomersMemoryOptimized
where LastName = 'White';
go