CREATE PROCEDURE getTableNames
AS
BEGIN
SELECT 
	s.name AS SchemaName, 
	t.name AS TableName
FROM 
	sys.tables t 
JOIN 
	sys.schemas s
ON t.schema_id = s.schema_id
WHERE t.name like '%dim' OR t.name = 'fact'
END
GO

EXEC getTableNames;