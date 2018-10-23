use dba
go

/*
drop table  checkdb_results
select * from checkdb_results
*/

alter proc db__checkdb_with_email_and_log @database_name sysname, @log_results bit = 0, @email varchar(255)=null -- exec db__checkdb_with_email_and_log @database_name = 'RestoreAudit', @log_results=1, @email='alwilson@ingeus.co.uk'
as 

---
-- Self Test that Parameters make sense
---
IF ISNULL(@database_name, 'NULL') = 'NULL'
BEGIN
RAISERROR (15600,-1,-1)
RETURN
END
---
---


---
-- Only if the user chose to log results to a table AND if the table doesn't already exist create it 
---
if @log_results != 0
begin
if object_id('checkdb_results') is null
create table checkdb_results
(
	checkdb_date datetime default getdate(),
	checkdb_run_id uniqueidentifier,
	error bigint,
	level int,
	state int,
	messagetext varchar(max) null,
	repairlevel nvarchar(max) null,
	status int,
	dbid int,
	dbFragid int,
	ObjectID int,
	IndexID int,
	PartitionID int,
	AllocUnitId int,
	RidDbId int,
	RidPruId int,
	[File]  int,
	[page] int,
	Slot int,
	RefDBId int,
	RefPruId int,
	RefFile int,
	RefPage int,
	RefSlot int,
	Allocation int
)
END

if object_id('tempdb..##checkdb_results') is not null
	drop table ##checkdb_results;

create table ##checkdb_results
(
	error bigint,
	level int,
	state int,
	messagetext varchar(max) null,
	repairlevel nvarchar(max) null,
	status int,
	dbid int,
	dbFragid int,
	ObjectID int,
	IndexID int,
	PartitionID int,
	AllocUnitId int,
	RidDbId int,
	RidPruId int,
	[File]  int,
	[page] int,
	Slot int,
	RefDBId int,
	RefPruId int,
	RefFile int,
	RefPage int,
	RefSlot int,
	Allocation int
)

declare @sql nvarchar(1000)
set @sql = 'dbcc checkdb(' + @database_name + ')  WITH TABLERESULTS';

insert ##checkdb_results(error,level,state,messagetext,repairlevel,status,dbid,dbFragid,ObjectID,IndexID,PartitionID,AllocUnitId,RidDbId,RidPruId,[File],[page],Slot,RefDBId,RefPruId,RefFile,RefPage,RefSlot,Allocation)
exec (@sql);


---
--  Only if the user chose to log the results to a table 
---
if @log_results != 0
begin
	declare @guid uniqueidentifier = newid()
	insert checkdb_results(checkdb_run_id,error,level,state,messagetext,repairlevel,status,dbid,dbFragid,ObjectID,IndexID,PartitionID,AllocUnitId,RidDbId,RidPruId,[File],[page],Slot,RefDBId,RefPruId,RefFile,RefPage,RefSlot,Allocation)
	select @guid,error,level,state,messagetext,repairlevel,status,dbid,dbFragid,ObjectID,IndexID,PartitionID,AllocUnitId,RidDbId,RidPruId,[File],[page],Slot,RefDBId,RefPruId,RefFile,RefPage,RefSlot,Allocation from ##checkdb_results
end

---
-- Only if they chose to send an email alert
---
IF (@email is not null)
begin

	declare @theSubject varchar(200) = 'DBCC Results: ' + @database_name

	EXEC msdb.dbo.sp_send_dbmail @recipients='alwilson@ingeus.co.uk',
	@subject = @theSubject,
	@query_result_header=0,
	@query = 'set nocount on; select messagetext from ##checkdb_results';
end

if object_id('tempdb..##checkdb_results') is not null
	drop table ##checkdb_results;

--select * from #checkdb_results
