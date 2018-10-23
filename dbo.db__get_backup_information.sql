
create procedure dbo.db__get_backup_information -- exec dbo.db__get_backup_information @type='l', @generation=1, @copy_only = 0
	@type char(1) = 'd',
	@generation int = 1,
	@copy_only tinyint = 0

	AS

SET NOCOUNT ON

PRINT 'GUID-type device or filenames indicate backups were taken by 3rd party product'

SELECT  DatabaseName = x.database_name,
        LastBackupFileName = x.physical_device_name,
        LastBackupDatetime = x.backup_start_date
FROM (  SELECT  bs.database_name,
                bs.backup_start_date,
                bmf.physical_device_name,
                  Ordinal = ROW_NUMBER() OVER( PARTITION BY bs.database_name ORDER BY bs.backup_start_date DESC )
          FROM  msdb.dbo.backupmediafamily bmf
                  JOIN msdb.dbo.backupmediaset bms ON bmf.media_set_id = bms.media_set_id
                  JOIN msdb.dbo.backupset bs ON bms.media_set_id = bs.media_set_id
          WHERE   bs.[type] = @type
                  AND bs.is_copy_only = @copy_only ) x
WHERE x.Ordinal = @generation
ORDER BY DatabaseName;