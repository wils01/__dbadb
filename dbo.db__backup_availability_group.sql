

CREATE PROC [dbo].[db__backup_availability_group] -- EXEC [db__backup_availability_group] @group_name = 'iwuk', @ROOT = 'D:\logs_test', @TYPE = 'null_log', @options = 'nounload'
	@group_name sysname = null,
	@ROOT varchar(1000),
	@TYPE varchar(10) = null,
	@Options varchar(200) = null
AS

SET NOCOUNT ON

---
-- Self Test that Parameters make sense
---
DECLARE @obj sysname

IF ISNULL(@TYPE, 'NULL') = 'NULL'
BEGIN
SET @obj = object_name(@@procid)
RAISERROR (15600,-1,-1, @obj)
RETURN
END

IF ISNULL(@group_name, 'NULL') = 'NULL'
BEGIN
SET @obj = object_name(@@procid)
RAISERROR (15600,-1,-1, @obj)
RETURN
END
---
---

IF SERVERPROPERTY ('IsHadrEnabled') = 1
BEGIN

DECLARE @COMMAND nvarchar(999);
DECLaRE @BACKUP_FILE_PATH varchar(999)

-- Path to Root Directory of Backups
--DECLARE @ROOT varchar(500)
--SET @ROOT = 'e:\backups\DBA\Backups\Logs'

-- Make sure @ROOT path is in a valid format
IF RIGHT(@ROOT, 1) != '\'
	SET @ROOT = @ROOT + '\'

IF OBJECT_ID('tempdb..#ActiveAvailabilityDatabases') IS NOT NULL
   DROP TABLE #ActiveAvailabilityDatabases

CREATE TABLE #ActiveAvailabilityDatabases
(
	[availability_database_name] sysname,
	[availability_group_name] sysname,
	[primary_server_name] sysname,
	[current_availability_group_role] sysname,
	[listener_name] sysname,
	[computed_backup_path] varchar(799),
	[computed_group_root] varchar(799)
)

INSERT #ActiveAvailabilityDatabases(availability_database_name, availability_group_name, primary_server_name, current_availability_group_role, listener_name, computed_backup_path, computed_group_root)

SELECT
   adc.database_name as [availability_database_name],
   AGC.name as [availability_group_name]
 , RCS.replica_server_name as [primary_server_name]
 , ARS.role_desc  as [current_availability_group_role]
 , AGL.dns_name  as [listener_name],
  @ROOT + agc.name + '\' + adc.database_name + '\' as [computed_backup_path],
  @ROOT + agc.name + '\' as [computed_group_root] 
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
 ARS.role_desc = 'PRIMARY'
 AND agc.name = @group_name

IF OBJECT_ID('tempdb..#directoryTree') IS NOT NULL
   DROP TABLE #directoryTree

CREATE TABLE #directoryTree
(
	subdirectory varchar(400), 
	depth int
);

INSERT #directoryTree
EXEC master..xp_dirtree @root 

DECLARE @database sysname,
		@path varchar(799),
		@group sysname,
		@grouproot varchar(500) 

DECLARE @DatabaseInGroup CURSOR 
SET @DatabaseInGroup = CURSOR 
FOR SELECT availability_database_name, computed_backup_path, availability_group_name, computed_group_root from #ActiveAvailabilityDatabases

OPEN @DatabaseInGroup

FETCH NEXT FROM @DatabaseInGroup
INTO @database, @path, @group, @grouproot

-- Append the Options Variable
-- And the exra comma needed
IF LEN(@options) > 0
BEGIN
	SET @options = ',' + @options
END

WHILE @@FETCH_STATUS = 0
	BEGIN
		
		
		/* If the directory exists and this is NOT a NUL: device backup carry on */
		IF  EXISTS(SELECT * FROM #directoryTree WHERE subdirectory = @group AND depth=1) AND EXISTS(SELECT * FROM #directoryTree WHERE subdirectory = @database AND depth=2 AND @type !='NULL_LOG')
		BEGIN

			DECLARE @NOW AS DATETIME
			SET @NOW = GETDATE();

			DECLARE @DATETAG AS VARCHAR(100)
			SET @DATETAG = (SELECT FORMAT(YEAR(@NOW), '0#') + FORMAT(MONTH(@now), '0#') + FORMAT(DAY(@now), '0#') + '-' + FORMAT(DATEPART(HOUR, @NOW), '0#') + FORMAT(DATEPART(MINUTE,@NOW), '0#') + FORMAT(DATEPART(SECOND,@NOW), '0#'))

			IF @TYPE = 'LOG'
			BEGIN
				SET @BACKUP_FILE_PATH = @ROOT +@group+'\'+@database + '\' + 'LOG_' + @database + '_' + @DATETAG + '.BAK'
				SET @COMMAND = 'BACKUP LOG ' + @database + ' TO DISK = ''' + @BACKUP_FILE_PATH + '''' + ' WITH COMPRESSION, STATS=25'
			END

			IF @TYPE = 'NULL_LOG'
			BEGIN
				SET @BACKUP_FILE_PATH = 'NUL:'
				SET @COMMAND = 'BACKUP LOG ' + @database + ' TO DISK = ''' + @BACKUP_FILE_PATH + ''''
			END

			IF @TYPE = 'DIFFERENTIAL' OR @TYPE = 'DIFF'
			BEGIN
				SET @BACKUP_FILE_PATH = @ROOT +@group+'\'+@database + '\' + 'DIFF_' + @database + '_' + @DATETAG + '.BAK'
				SET @COMMAND = 'BACKUP DATABASE ' + @database + ' TO DISK = ''' + @BACKUP_FILE_PATH + +'''' + ' WITH COMPRESSION, DIFFERENTIAL, STATS=25' + ISNULL(@options, ';')
			END
			
			IF @TYPE = 'FULL'
			BEGIN
				SET @BACKUP_FILE_PATH = @ROOT +@group+'\'+@database + '\' + 'FULL_' + @database + '_' + @DATETAG + '.BAK'
				SET @COMMAND = 'BACKUP DATABASE ' + @database + ' TO DISK = ''' + @BACKUP_FILE_PATH + '''' + ' WITH COMPRESSION, STATS=25' + ISNULL(@options, ';')
			END
			
	
		END

		/* Otherwise handle null device backups and bad directory-paths here */
		ELSE

		BEGIN
			IF @type != 'NULL_LOG'
			begin
			PRINT ' Aborting ' + (@type) + ' backup for ' + (@database) + '. Because directory not found: ' + @path + CHAR(13)
			end
			else
			begin
				SET @BACKUP_FILE_PATH = 'NUL:'
				SET @COMMAND = 'BACKUP LOG ' + @database + ' TO DISK = ''' + @BACKUP_FILE_PATH + '''' + ' WITH STATS=25 '
			end
		END
		
		/* 
			DoWork()
		*/
		PRINT '>>>'
		PRINT @command
		exec master..sp_executesql @command;
		PRINT '<<<'
		
		FETCH NEXT FROM @DatabaseInGroup 
		INTO @database, @path, @group, @grouproot

		
	END; 

CLOSE @DatabaseInGroup 
DEALLOCATE @DatabaseInGroup

END

ELSE

BEGIN
PRINT 'Cannot Backup Availability Groups when HADR has not been enabled.'
END



GO

