
alter proc db__create_database_snapshot -- exec  db__create_database_snapshot @SourceDatabase = 'MessageBroker', @execute=1, @append_datetime=0
	@SourceDatabase sysname,
	@FilePath varchar(1000)	= null,
	@Execute bit = 0,
	@Print bit = 1,
	@append_datetime bit =1
as


set nocount on

-- Generate DATETIME as YYYMMDD__HHMM
declare @current_datetime_tag varchar(30) = convert(VARCHAR(30), GETDATE(), 112) + '__' + REPLACE(CONVERT(varchar(5), GETDATE(), 108), ':', '')

IF DB_ID(@SourceDatabase) IS NULL
	RAISERROR('Database doesn''t exist!',1,1)
	
IF @FilePath = ''
	SET @FilePath = NULL

	declare @filesql nvarchar(1000) = ''
	SELECT @FileSql = @FileSql +
	CASE -- Case statement used to wrap a comma in the right place.
		WHEN @FileSql <> '' 
		THEN + ','
		ELSE ''
	END + '(NAME = ' + mf.name + ', FILENAME = ''' + ISNULL(@FilePath, LEFT(mf.physical_name,LEN(mf.physical_name)- 4 ) ) + '_' + @current_datetime_tag + '_' + cast([file_id] as varchar(10)) + '.ss'')'
FROM sys.master_files AS mf
	INNER JOIN sys.databases AS db ON db.database_id = mf.database_id
WHERE db.state = 0 -- Only include database online.
AND mf.type = 0 -- Only include data files.
AND db.[name] = @SourceDatabase


set @filesql = 'CREATE DATABASE ' 
					+ 'SS_' + @SourceDatabase 
					+ case @append_datetime when 1 then '_'+@current_datetime_tag else '' end
					+ ' ON'
					+ ' ' + @filesql
					+ ' AS SNAPSHOT OF ' + @SourceDatabase


if @Print = 1
begin
print @filesql
end

if @Execute = 1
begin
exec sp_executesql @filesql
end

