
/*
declare @sql nvarchar(max)

exec db__generate_restore_move 
	@path_to_backup = 'c:\temp\test_bak.bak', 
	@new_restore_data_path = 'c:\temp\',  
	@new_restore_log_path='d:\temp\', 
	@recovery_type = 'recovery',
	@sqlstatement = @sql output

	SELECT @sql as [statement]
*/


create proc [dbo].[db__generate_restore_move] 
	@path_to_backup varchar(1000),
	@new_restore_data_path varchar(1000) = '',
	@new_restore_log_path varchar(1000) = '',
	@recovery_type varchar(30) = 'norecovery',
	@sqlstatement nvarchar(max) output

AS 
	
SET NOCOUNT ON

IF OBJECT_ID('tempdb..#filelistonly') IS NOT NULL
    DROP TABLE #filelistonly

if right(@new_restore_data_path,1) != '\'
	begin
		set @new_restore_data_path = @new_restore_data_path + '\'
	end

if right(@new_restore_log_path,1) != '\'
	begin
		set @new_restore_log_path = @new_restore_log_path + '\'
	end


declare @backup_header_only TABLE 
(
	backupname varchar(400),
	backupdescription varchar(500),
	backuptype int,
	expriationdate datetime,
	compressed tinyint,
	position int,
	devicetype int,
	username varchar(100),
	servername varchar(100),
	databasename varchar(400),
	databaseversion int,
	databasecreationdate datetime,
	backupsize bigint,
	fistlsn varchar(300),
	lastlsn varchar(300),
	checkpointlsn varchar(300),
	databasebackuplsn varchar(300),
	backupstartdate datetime,
	backupfinishdate datetime,
	sortorder int,
	[codepage] int,
	unicodelocaleid int,
	unicodecomparisonstyle bigint,
	compatibilitylevel int,
	softwarevendorid int,
	softwareversionmajor int,
	softwareversionminor int,
	softwareversionbuild int,
	machinename varchar(200),
	flags int,
	bindingid varchar(100),
	recoveryforkid varchar(100),
	collation sysname,
	familyguid varchar(100),
	hasbulkloggeddata int,
	issnapshot int,
	isreadonly int,
	issingleuser int,
	hasbackupchecksums int,
	isdamaged int,
	beginlogchain int,
	hasincompletemetadata int,
	isforceoffline int,
	iscopyonly int,
	firstrecoveryforkid varchar(100),
	forkpointlsn varchar(100),
	recoverymodel varchar(400),
	differentialbaselsn varchar(100),
	differentialbaseguid varchar(100),
	backuptypedescription varchar(400),
	backupsetguid varchar(100),
	compressedbackupsize bigint,
	containment int,
	keyalgorithm nvarchar(32),
	EncryptorThumbprint varbinary(20),
	EncryptorType nvarchar(32)


)


declare @sql_headeronly varchar(1000) = 'restore headeronly from disk = ' + quotename(@path_to_backup, '''')
--print @sql_headeronly

insert @backup_header_only
exec (@sql_headeronly)

create table #filelistonly 
(
	logicalname sysname, 
	physicalname sysname,
	[type] char(1),
	filegroupname sysname null,
	size varchar(200),
	maxsize varchar(200),
	fileid int,
	createlsn varchar(200),
	droplsn varchar(200),
	uniqueid varchar(200),
	readonlylsn int,
	readwritelsn int,
	backupsizeinbytes varchar(200),
	sourceblocksize int,
	filegroupid int,
	loggroupguid varchar(200),
	differentialbaselsn varchar(200),
	differentialbaseguid varchar(200),
	isreadonly varchar(200),
	ispresent varchar(200),
	tdethumbprint varchar(200),
	snapshoturl varchar(555)
)


set @sqlstatement=''

declare @sql_filelistonly varchar(200)
set @sql_filelistonly = 'restore filelistonly from disk = ''' + @path_to_backup + ''''
insert #filelistonly
exec (@sql_filelistonly)

declare @database_name_from_backup varchar(100)
select top 1 @database_name_from_backup = databasename from @backup_header_only

/* start building the output parameter for consumin downstream */
set @sqlstatement=@sqlstatement + 'restore database ' + quotename(@database_name_from_backup)
set @sqlstatement=@sqlstatement + ' from disk = ' + '''' + @path_to_backup + ''''
set @sqlstatement=@sqlstatement +  ' with stats=1,'

/* start printing the statement to the screen for human use */
print 'restore database ' + quotename(@database_name_from_backup)
print 'from disk = ' + '''' + @path_to_backup + ''''
print 'with stats=1,'


declare @logicalname varchar(200), @physicalname varchar(500), @file_type varchar(2)

DECLARE logical_files_formove CURSOR FOR  
select logicalname, physicalname, [type] from #filelistonly

OPEN logical_files_formove   
FETCH NEXT FROM logical_files_formove INTO @logicalname, @physicalname, @file_type   

WHILE @@FETCH_STATUS = 0   
BEGIN   
       if @file_type = 'D'
	   begin 
		set @sqlstatement=@sqlstatement + 'move ' + '''' + @logicalname + '''' + ' to ' + '''' + @new_restore_data_path + RIGHT(@physicalname,CHARINDEX('\',REVERSE(@physicalname))-1) +  '''' + ','
		print 'move ' + '''' + @logicalname + '''' + ' to ' + '''' + @new_restore_data_path + RIGHT(@physicalname,CHARINDEX('\',REVERSE(@physicalname))-1) +  '''' + ','
	   end

	   if @file_type='L'
	   begin
	   set @sqlstatement=@sqlstatement + 'move ' + '''' + @logicalname + '''' + ' to ' + '''' + @new_restore_log_path + RIGHT(@physicalname,CHARINDEX('\',REVERSE(@physicalname))-1) +  '''' + ','
	   print 'move ' + '''' + @logicalname + '''' + ' to ' + '''' + @new_restore_log_path + RIGHT(@physicalname,CHARINDEX('\',REVERSE(@physicalname))-1) +  '''' + ','
	   end


       FETCH NEXT FROM logical_files_formove INTO @logicalname, @physicalname  , @file_type   

END   

CLOSE logical_files_formove   
DEALLOCATE logical_files_formove

print @recovery_type
 set @sqlstatement=@sqlstatement + @recovery_type

--select * from #filelistonly

SET NOCOUNT OFF

go


