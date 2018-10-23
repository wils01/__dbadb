
-- exec dbo.db__restore_all_backups_from_directory @source_directory='d:\backup\adapts', @restore_data_directory='d:\data\adapts', @restore_log_directory='L:\Logs\adapts', @recovery_type='recovery'
alter proc dbo.db__restore_all_backups_from_directory  
	@source_directory varchar(1000),
	@restore_data_directory varchar(1000),
	@restore_log_directory varchar(1000),
	@recovery_type varchar(30) = 'norecovery'

AS

set nocount on

declare @list_of_backups TABLE (path_to_backup varchar(1000));

-- Make sure source and restore path's 
-- are valid with a trailing slash
if left(@source_directory, 1) != '\'
begin
	set @source_directory = @source_directory + '\'
end

if left(@restore_data_directory, 1) != '\'
begin
	set @restore_data_directory = @restore_data_directory + '\'
end
--
--

declare @x varchar(1000) = 'EXEC master..xp_cmdshell ' + quotename('dir ' + @source_directory + '*.bak' + '/B', '''') 
print '/*'
print @x
print '*/'

-- Populate table with list of backup files found
insert @list_of_backups(path_to_backup)
EXEC (@x)

-- Remove trailing blank rows
delete @list_of_backups where path_to_backup is null

-- append path to create a full filename
update @list_of_backups set path_to_backup = @source_directory+path_to_backup

declare @the_current_backup sysname
DECLARE all_backups_to_restore CURSOR FOR 
select path_to_backup from  @list_of_backups

open all_backups_to_restore
FETCH NEXT FROM all_backups_to_restore INTO @the_current_backup
WHILE @@FETCH_STATUS = 0   
BEGIN   
       PRINT '-- Begin Restore Statement for: ' + @the_current_backup
	   
	   exec dbo.db__generate_restore_move @the_current_backup, @restore_data_directory, @restore_log_directory, @recovery_type
	   
	   PRINT '-- End Statement'
	   PRINT ''

	   FETCH NEXT FROM all_backups_to_restore INTO @the_current_backup   
END   



CLOSE all_backups_to_restore   
DEALLOCATE all_backups_to_restore
--select * from @list_of_backups

set nocount off