use dba
go


alter PROC [dbo].[db__checkdb_availability_group] -- EXEC [db__checkdb_availability_group] @group_name = 'iwhp'
	@group_name sysname = null
AS

SET NOCOUNT ON

---
-- Self Test that Parameters make sense
---
IF ISNULL(@group_name, 'NULL') = 'NULL'
BEGIN
RAISERROR (15600,-1,-1)
RETURN
END
---
---

IF SERVERPROPERTY ('IsHadrEnabled') = 1
BEGIN


CREATE TABLE #ActiveAvailabilityDatabases
(
	[availability_database_name] sysname,
	[availability_group_name] sysname
	
)

INSERT #ActiveAvailabilityDatabases(availability_database_name, availability_group_name)

SELECT
   adc.database_name as [availability_database_name],
   AGC.name as [availability_group_name]
  FROM sys.availability_groups_cluster AS AGC
  INNER JOIN sys.dm_hadr_availability_replica_cluster_states AS RCS
   ON RCS.group_id = AGC.group_id
  INNER JOIN sys.dm_hadr_availability_replica_states AS ARS
   ON ARS.replica_id = RCS.replica_id
  INNER JOIN sys.availability_group_listeners AS AGL
   ON AGL.group_id = ARS.group_id
   INNER JOIN sys.availability_databases_cluster as ADC
   ON AGL.group_id=ADC.group_id
WHERE
 --ARS.role_desc = 'PRIMARY'
 --AND 
 agc.name = @group_name



DECLARE @database sysname,
		@path varchar(799),
		@group sysname,
		@grouproot varchar(500) 

DECLARE @DatabaseInGroup CURSOR 
SET @DatabaseInGroup = CURSOR 
FOR SELECT availability_database_name, availability_group_name from #ActiveAvailabilityDatabases

OPEN @DatabaseInGroup

FETCH NEXT FROM @DatabaseInGroup
INTO @database, @group

WHILE @@FETCH_STATUS = 0
	BEGIN
		
		/* Do work here */
		print 'Date: ' + cast(GETDATE() as varchar(20)) + ': Checking ' + @database
		exec db__checkdb_with_email @database_name = @database
				
		FETCH NEXT FROM @DatabaseInGroup 
		INTO @database, @group

		
	END; 

CLOSE @DatabaseInGroup 
DEALLOCATE @DatabaseInGroup

END

ELSE

BEGIN
PRINT 'Cannot Perform CheckDB on an Availability Groups when HADR has not been enabled.'
END


