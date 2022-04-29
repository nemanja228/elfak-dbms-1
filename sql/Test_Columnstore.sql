use Columnstore
go

-- Test query with forced rowstore index usage
select a.ArticleCode, sum(s.Amount) as [TotalAmount]
from dbo.FactSales s
with (index = 1)
join dbo.DimArticles a on s.ArticleId = a.ArticleId
group by a.ArticleCode

-- Test query using columnstore (optimal)
select a.ArticleCode, sum(s.Amount) as [TotalAmount]
from dbo.FactSales s
join dbo.DimArticles a on s.ArticleId = a.ArticleId
group by a.ArticleCode
