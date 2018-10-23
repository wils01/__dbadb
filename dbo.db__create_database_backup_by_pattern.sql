GO
alter procedure db__create_database_backup_by_pattern -- exec dba..db__create_database_backup_by_pattern @type='f', @backup_path= '\\172.16.7.199\dev\Backups\GLSQCOL1003\Backups\', @pattern='', @token='', @print_only=1, @date_stamp=0, @overwrite_existing = 1, @skip_system=0, @all_databases=1, @single_database = null;
	@type char(1),
	@backup_path varchar(1000),
	@pattern varchar(500),
	@token varchar(30) = '',
	@print_only bit = 1,
	@date_stamp bit = 1,
	@overwrite_existing bit = 0,
	@skip_system bit = 1,
	@all_databases bit = null,
	@single_database sysname = null,
	@availability_group sysname = null
AS

/*
Usage: 
@type - (d)ifferental, (f)ull [logs not supported yet] 
@backup_path - the root folder where the backup will be created
@pattern - a wildcard/like match in sysdatabases equivalent to '%pattern%'
@token - inserted into the filename of the backup after the type tag -- can be anything helpful
*/


-----------------------------------------------------
-- Check Inputs are Valid SUMMING Conflicting Input
-- Values > 1 are Conflicting and we should abort
-----------------------------------------------------
declare @obj sysname
declare @database_input_count smallint = 0;

if @all_databases != 0 or @all_databases is not null 
	set @database_input_count = @database_input_count+1

if @single_database is not null 
	set @database_input_count = @database_input_count+1

if @availability_group is not null 
	set @database_input_count = @database_input_count+1

if @database_input_count > 1
BEGIN
SET @obj = object_name(@@procid)
RAISERROR (15600,-1,-1, @obj)
RETURN
END
-----------------------------------------
--End Check
-----------------------------------------

if right(@backup_path,1) != '\'
	begin
		set @backup_path = @backup_path + '\'
	end


-----------------------------------------
-- Display Input Parameters Chosen
-----------------------------------------
print '/*'
print ''
print 'Matching: LIKE ' + '%' + @pattern + '%'
Print 'Action: ' + case @print_only when 1 then 'Scripting Only' else 'Executing Backup' end
print 'Backup Directory: '+ @backup_path
print 'Backup Type: ' + case @type when 'f' then 'FULL' when 'd' then 'DIFFERENTIAL' END
print 'Overwrite Existing?: ' + case @overwrite_existing when 1 then 'YES' else 'NO' END
if @single_database is not null print 'Single Database Option Specified: Ignoring PATTERN'
if @availability_group is not null print 'Availability Group Option Specified: ' + @availability_group
if @all_databases is not null print 'Backup All Databases ' + case @skip_system when 1 then 'Skipping System Databases' else 'Including System Databases' END
if @date_stamp is not null print 'Appending CURRENT DATE to Filenames'
if (@token is null) or len(@token) < 1 
begin
	print 'Appending Nothing Extra to Filenames'
end
else
begin
	print 'Appending ' + @token + ' to Filenames'
end

print ''
print '*/'
print ''
-----------------------------------------
-- End of Informational Message

-----------------------------------------
declare @backup_type varchar(30) 
SET @backup_type= case @type 
	when 'd' then ', differential'
	when 'f' then ''
	end

declare @backup_tag varchar(5)
SET @backup_tag= case @type 
	when 'd' then 'diff'
	when 'f' then 'full'
	end

declare @date_tag varchar(12)

if @date_stamp = 1
begin
	set @date_tag = '_' + CONVERT(varchar(8), GETDATE(), 112)
end
else
begin
	set @date_tag = ''
end

declare @matched_db_name varchar(200)

declare @tsql nvarchar(4000)
set @tsql = ''


IF OBJECT_ID('tempdb..#worklist') IS NOT NULL 
	DROP TABLE #worklist

create table #worklist
(
	[name] sysname
)


-----------------------------------------
-- Populate WORKLIST for All Databases
-----------------------------------------
if @all_databases = 1
begin
 insert #worklist([name])
 select name from master.sys.databases
 where name like '%' + @pattern + '%'
 and [database_id] > case @skip_system when 1 then 4 else 0 end
 and [name] != 'tempdb'
end


-----------------------------------------
-- Populate WORKLIST for Single Database
-----------------------------------------
if (@single_database is not null)
begin
 insert #worklist([name])
 values(@single_database)
end


--------------------------------------------
-- Populate WORKLIST for Availability Group
-- This option still uses @PATTERN to match
--------------------------------------------
IF (@availability_group is not null)
BEGIN
	IF SERVERPROPERTY ('IsHadrEnabled') = 1
	BEGIN
		insert #worklist([name])
		SELECT d.database_name --, g.name
		FROM sys.availability_databases_cluster d
		inner join sys.availability_groups g on d.group_id=g.group_id
		where g.name = @availability_group
		and g.name like '%' + @pattern + '%'
	END
 END




DECLARE all_matching_databases CURSOR FOR  
select [name] from #worklist

OPEN all_matching_databases

FETCH NEXT FROM all_matching_databases INTO @matched_db_name  
  
WHILE @@FETCH_STATUS = 0   
BEGIN   
	
	set @tsql = @tsql + '-- Begin Database Backup Script for ' + @matched_db_name + char(13)
	set @tsql = @tsql +  'backup database ' + quotename(@matched_db_name) + char(13)
	set @tsql = @tsql + 'to disk = ' + quotename(@backup_path + @backup_tag + @token + '_' +  @matched_db_name + @date_tag + '.BAK', '''') + char(13)
	set @tsql = @tsql + 'with stats=1, compression' + @backup_type
	set @tsql = @tsql + case @overwrite_existing when 0 then '' else ' ,init' end + char(13)
	set @tsql = @tsql + '-- End Script'
	set @tsql = @tsql + char(13)
		
	if @print_only != 1
	begin
		print @tsql;
		exec sp_executesql @tsql
	end
	else
	begin
		print @tsql
	end

	set @tsql=''

FETCH NEXT FROM all_matching_databases INTO @matched_db_name  
END


CLOSE all_matching_databases   
DEALLOCATE all_matching_databases




