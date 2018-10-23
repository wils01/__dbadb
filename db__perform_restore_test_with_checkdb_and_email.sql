


-- select * from restore_list

--delete top (5)  from restore_list


--
go
create proc dbo.db__perform_restore_test_with_checkdb_and_email
as


declare @database_name sysname, @path_to_backup varchar(1000), @source_server varchar(100), @restore_path_for_move varchar(200)
declare @tsql nvarchar(1000)

DECLARE backup_cursor CURSOR FOR 
SELECT [database_name], path_to_backup, source_server, restore_path_for_move
FROM restoreaudit..restore_list
WHERE [status] =1

OPEN backup_cursor  
FETCH NEXT FROM backup_cursor INTO @database_name, @path_to_backup, @source_server, @restore_path_for_move

WHILE @@FETCH_STATUS = 0  
BEGIN  
      
	  exec db__generate_restore_move 
		@path_to_backup = @path_to_backup, 
		@new_restore_data_path = @restore_path_for_move,  
		@new_restore_log_path=@restore_path_for_move, 
		@recovery_type = 'recovery',
		@sqlstatement = @tsql output
	  
      --BACKUP DATABASE @name TO DISK = @fileName 


	  declare @dropTsql nvarchar(300) = 'drop database ' + @database_name

	  -- If it already exists drop the database here before restoring a new copy. 
	  if db_id(@database_name) is not null
	  begin
		
		print 'Dropping Existing Database...'
		exec sp_executesql @dropTsql;
	  end


	  print 'Performing Test Restore...'
	  print @tsql;
	  exec sp_executesql @tsql

	  print 'Performing Integrity Check'
	  exec dba..db__checkdb_with_email_and_log @database_name = @database_name, @log_results=1, @email='alwilson@ingeus.co.uk'



	  -- drop the database again to clean up after yourself to save on storage required per test-run
	   if db_id(@database_name) is not null
	  begin
		print 'Dropping Database Because of End of Run...'
		exec sp_executesql @dropTsql;
	  end


      FETCH NEXT FROM backup_cursor INTO @database_name, @path_to_backup, @source_server, @restore_path_for_move
END 

CLOSE backup_cursor  
DEALLOCATE backup_cursor 


