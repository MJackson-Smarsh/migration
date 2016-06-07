<# Will read in a file of databases and take that and generate a restore script then sftp it over to vegas.
script it generates will restore fulls but without recovery, so they will be awaiting the diff.  Please note, there is NO
automatino as to where the files will go on the new prod instance, you will still need to comment / un-comment the sql below in strsql variable


#>

function run_sql {


 [CmdletBinding()]
    Param(
    [Parameter(Mandatory=$true)] [string]$datafilelocation,
    [Parameter(Mandatory=$true)] [string]$bulkfile,
    [Parameter(Mandatory=$true)] [string]$logdir
        )
$strsql="
use msdb
--declare @DatabaseList table (DatabaseName varchar(100))
declare @DatabaseBackups table (DatabaseName varchar(100), BackupType char(4), FileLocation varchar(500), [Rank] int)
declare @DatabaseFiles table (DatabaseName varchar(100), BackupType char(4), FileType varchar(4), LogicalName varchar(100), [FileName] varchar(100), [Rank] int)
--declare @Script varchar(max),
declare		@script2 varchar(max),
		@DatabaseTarget varchar(100),
		@DatafileDestination varchar(500),
		@LogfileDestination varchar(500)
		DECLARE @PQgroupname VARCHAR(50)   --this is the powerquery group we will no longer need  = 'amer\ISusrP-' + DB_NAME() + '_Acct_Mgr'

set nocount on

---set @Script = ''
set @script2 = ''

 --set @DatafileDestination = 'F:\SQL02_DATA_MP04\MSSQL\DATA\'
 set @LogFileDestination = 'F:\SQL02_Logs_MP01\MSSQL\Logs\'
 --set @datafileDestination = '$datafilelocation'
--to restore to P054ECASQQLP01\ECAPREVIEW
--set @DatafileDestination = 'M:\Data\MSSQL2014.ECAPREVIEW\Data\'
--set @LogFIleDestination = 'L:\Logs\MSSQL2014.ECAPREVIEW\Logs\'


if exists (select * from INFORMATION_SCHEMA.Tables where table_name = '#tmpdbnames')
	drop table #tmpdbnames;

create table #tmpDBnames (
	dbname nvarchar(250)
	)
	
bulk insert #tmpdbnames FROM '$bulkfile'
with (ROWTERMINATOR = '\n')



/*****  Create restore from backup statements for every User database  
        Relativity servers Only 
	
	Created by Gary Hamm
	Create Date: 5/4/2016

*****/

SET nocount on

--- DBA supplied variables
--  @SrcInst   = Current SQL Server Instance
--  @RestoreInst = Targart SQL Server Instance
--  @Dataloc  = Target SQL Server Default physical Data file Location
--  @Dataloc2-10  = Target SQL Server Alternate physical Data file Locations  (for Relativity striping or large DB's or balancing)
--  @Dataloc2  = Original use: for distribiting large DB's / physical file balancing)
--  @Logloc  = Target SQL Server Default Logs file Location.  vars ..3-10 not present on non Relativity version of script.
--  @DatalocFTI = Target Relativity FTI physical files location
--  @UseAltDataloc  Toggle variable  0 = No (Default) 1 = Yes (size based)  2 = (balanced -every other database)
--  @UseAltDatalocSize  = Size threshhold (e.g. DB's  > X will be placed on @Dataloc2
--  @IncludeDBs  = 'ALL' (default), 'LIST'  -- If using List, you must populate it.  See details within script.


DROP TABLE ##vDatabaseFiles

DECLARE  @srcInst varchar(100)
DECLARE  @restoreInst varchar(100)
DECLARE  @Dataloc varchar(100) = ''
DECLARE  @Dataloc2 varchar(100)
DECLARE  @Dataloc3 varchar(100)
DECLARE  @Dataloc4 varchar(100)
DECLARE  @Dataloc5 varchar(100)
DECLARE  @Dataloc6 varchar(100)
DECLARE  @Dataloc7 varchar(100)
DECLARE  @Dataloc8 varchar(100)
DECLARE  @Dataloc9 varchar(100)
DECLARE  @Dataloc10 varchar(100)
DECLARE  @DatalocFTI varchar(100)
DECLARE  @logloc varchar(100)
DECLARE  @BKType char(1)  -- D = Full, I = Diff
DECLARE  @UseAltDataloc INT
DECLARE  @UseAltDatalocSize INT
DECLARE  @IncludeDBs  varchar(4)  -- 'ALL' (default), 'LIST'  -- If List - you must populate it.

--SET @SrcInst = 'P064SQLCI0202\REL02'
SET @SrcInst = 'ED017-HSRLCLI03\RELATIVITY02'
SET @RestoreInst = 'P064SQLCI0201\REL01'
SET @Dataloc = '$datafilelocation'
SET @Dataloc2 = 'F:\REL01_DATA_MP02\MSSQL12.REL01\MSSQL\Data'
SET @Dataloc3 = 'F:\REL01_DATA_MP03\MSSQL12.REL01\MSSQL\Data'
SET @Dataloc4 = 'F:\REL01_DATA_MP04\MSSQL12.REL01\MSSQL\Data'
--SET @Dataloc5 = 'F:\REL01_DATA_MP05\MSSQL12.REL01\MSSQL\Data'
--SET @Dataloc6 = 'F:\REL01_DATA_MP06\MSSQL12.REL01\MSSQL\Data'
--SET @Dataloc7 = 'F:\REL01_DATA_MP07\MSSQL12.REL01\MSSQL\Data'
--SET @Dataloc8 = 'F:\REL01_DATA_MP08\MSSQL12.REL01\MSSQL\Data'
--SET @Dataloc9 = 'F:\REL01_DATA_MP09\MSSQL12.REL01\MSSQL\Data'
--SET @Dataloc10 = 'F:\REL01_DATA_MP10\MSSQL12.REL01\MSSQL\Data'
--SET @DatalocFTI = 'P:\MSSQL\FTData'
SET @Logloc = '$logdir'
SET @UseAltDataloc = 0  --0  -- 1 -- 2
SET @UseAltDatalocSize = 50000
SET @BKType = 'D'
--SET @BKType = 'I'
SET @IncludeDBs = 'ALL'
SET @IncludeDBs = 'LIST'

-- populate databaselist and process it
IF upper(@IncludeDBs) = 'LIST'
BEGIN
	DECLARE  @DBProcessList table (Name varchar(100))
				--	, Size_MB bigint
				--	, TargetInst varchar(100)
				--	)

	-- DBA - Modify / provide list of databases to script out.
    --set @DBPRocesslist = (select * From #tmpdbnames)
	--insert into @DBProcessList (Name) --, Size_MB, TargetInst)
	insert into  @DBProcessList (name)  select * From #tmpdbnames

END


-- Internal Script Variables
DECLARE  @BkName varchar(500)
DECLARE  @DatabaseList table (DatabaseName varchar(100)
				, Size_MB bigint
				)
--DECLARE  @DatabaseFiles table 
--		(DatabaseName varchar(100)
--		, Logical_Name varchar(100)
--		, Physical_name varchar(300)
--		, File_name varchar(100)
--		, File_id Int
--		)

DECLARE  @Script varchar(max)	-- 
DECLARE  @DBCount int		-- Qty of Databases
DECLARE  @FileLooper int	-- Looping variable
DECLARE  @DBTarget varchar(100)	-- Target Database
DECLARE  @currDBSize INT   	-- Current db size to compare to @UseAltDatalocSize variable
DECLARE  @currDataloc varchar(100)
DECLARE  @MediaID int
DECLARE  @FirstFam int
DECLARE  @LastFam int


IF upper(@IncludeDBs) <> 'LIST' 
	BEGIN
		-- Gather list of User databases on source server
		insert into @DatabaseList (DatabaseName, Size_MB)
			SELECT d.name AS DatabaseName,
			ROUND(SUM(mf.size * 8 / 1024) , 0) Size_MB
			--0 Size_MB
			FROM sys.master_files mf
			INNER JOIN sys.databases d ON d.database_id = mf.database_id
			WHERE d.name <> N'master'
		  and d.name <> N'tempdb'
		  and d.name <> N'model'
		  and d.name <> N'msdb'
		  and d.name <> N'DBASupport'
		--  and d.name = 'CA6336'
			GROUP BY d.name
		--	ORDER BY Size_MB desc
		SET @DBCount = @@rowcount
	END
ELSE   --  (@IncludeDBs) = 'LIST'  -- this option uses a database process list created / maintained by the DBA
	BEGIN
		insert into @DatabaseList (DatabaseName) --, Size_MB)
		SELECT name AS DatabaseName
		--	, Size_MB
		FROM @DBProcessList
		--WHERE TargetInst = @RestoreInst   -- Defaults to the instance variable supplied by the DBA at the start of the script.
		SET @DBCount = @@rowcount
	END

--select DatabaseName
--from @DatabaseList
--sET @DBCount = @@rowcount


--insert into @DatabaseList (DatabaseName)
--select name 
--from sys.databases
--where name <> N'master'
--  and name <> N'tempdb'
--  and name <> N'model'
--  and name <> N'msdb'
--  and name <> N'DBASupport'
--sET @DBCount = @@rowcount

Print '-- Total qty of User Databases: ' + CONVERT(VARCHAR(6),@DBCount)
-- End ofGather list of User databases on source server


-- Gather list of physical files in each database
CREATE TABLE  ##vDatabaseFiles 
		(DatabaseName varchar(100)
		, File_id Int
		, Logical_Name varchar(100)
		, File_name varchar(100)
		, Physical_name varchar(300)
		)
DECLARE @DBListCount INT

IF upper(@IncludeDBs) = 'LIST' 
	BEGIN
		Exec sp_msforeachdb '
		Use [?];
			INSERT INTO ##vDatabaseFiles 
			(DatabaseName, File_id, Logical_Name, File_name, Physical_name) 
			select
				 DB_NAME() AS DatabaseName
				, file_id
				, name AS Logical_Name
				, RIGHT(physical_name,CHARINDEX(''\'', REVERSE(physical_name))-1) AS  File_name
				, Physical_name
			from sys.database_files
			order by file_id'
	END
ELSE
	BEGIN
		DECLARE  @DBSubProcessList table (Name varchar(100))
		DECLARE @DBListLooper INT
		DECLARE @CurrListDB varchar(100)
		DECLARE @vcmd VARCHAR(2000)

		INSERT INTO @DBSubProcessList 
		Select [Name]
		from @DBProcessList
		SET @DBListLooper = @@rowcount
		while @DBListLooper > 0
			begin
				select top 1 @CurrListDB = Name from @DBSubProcessList
				set @vcmd = 'select ''' +
					@CurrListDB + ''' AS DatabaseName
					, file_id
					, name AS Logical_Name
					, RIGHT(physical_name,CHARINDEX(''\'', REVERSE(physical_name))-1) AS  File_name
					, Physical_name
				from [' + @CurrListDB + '].sys.database_files
				order by file_id'
				exec (@vcmd)

				delete from @DBSubProcessList
				where [Name] = @CurrListDB
				SET @DBListLooper = @DBListLooper  - 1
			end
	END
-- End of Gather list of physical files in each database

--select * from ##vDatabaseFiles 
-- End of Gather list of physical files in each database



-- Create Restore statements
DECLARE @DBLooper INT
SET @DBLooper = @DBCount
while @DBLooper > 0
begin

	-- Get next database in the DB list
	-- Order by @UseAltDataloc setting  0 = No (Default) 1 = Yes (size based)  2 = (balanced -every other database)
	IF @UseAltDataloc = 0
	BEGIN	
		SET @DBTarget = ''
		select top 1 @DBTarget = DatabaseName
		from @DatabaseList
	END
	IF @UseAltDataloc = 1
	BEGIN	
		SET @DBTarget = ''
		select top 1 @DBTarget = DatabaseName
		from @DatabaseList
		ORDER BY Size_MB desc
	END
	IF @UseAltDataloc = 2
	BEGIN	
		select top 1 @DBTarget = DatabaseName
		from @DatabaseList
		ORDER BY Size_MB desc
	END
	IF @UseAltDataloc = 3   -- Databse Process list
	BEGIN	
		SET @DBTarget = ''
		select top 1 @DBTarget = DatabaseName
		from @DatabaseList
	END
	-- Query for last backup
	SELECT TOP 1 @bkname = m.physical_device_name
		, @MediaID = m.media_set_id
		, @FirstFam = s.first_family_number
		, @LastFam = s.last_family_number
	FROM msdb.dbo.backupSET s
	INNER JOIN msdb.dbo.backupmediafamily m
		ON s.media_SET_id = m.media_SET_id
	WHERE s.database_name = @DBTarget --IN ('EquivioZoom')
	AND s.type  = @BKType -- IN (' + @BKType + ')  -- 'D'
	 --and s.type = 'L'
	ORDER BY s.backup_finish_date DESC


	-- Get DB size info for possible alternate datalocation.
	--DECLARE @currDBSize INT
	SET @currDBSize = 0
	SELECT --d.name,
	@currDBSize =  ROUND(SUM(mf.size * 8 / 1024), 0) -- Size_MBs
	FROM sys.master_files mf
	INNER JOIN sys.databases d ON d.database_id = mf.database_id
	WHERE d.name =  @DBTarget  --d.database_id > 4 -- Skip system databases
	--GROUP BY d.name
	--ORDER BY d.name
	print '-- Processing ' + @DBTarget  + ' Current Size (MB): ' + convert(varchar(20),@currDBSize) 

	-- Get DB size info for possible alternate datalocation.

	-- Set current data base Data files location
	--  @UseAltDataloc  = 0 = No (Default) 1 = Yes (size based)  2 = (balanced -every other database)
	print '-- Alt Data Loc value is: ' + convert(varchar(20),@UseAltDataloc)
	
--	IF @UseAltDataloc = 

	IF @UseAltDataloc = 0 SET @currDataloc = @Dataloc
	IF @UseAltDataloc = 1 and (@currDBSize < @UseAltDatalocSize)  SET @currDataloc = @Dataloc
	IF @UseAltDataloc = 1 and (@currDBSize >= @UseAltDatalocSize) SET @currDataloc = @Dataloc2
	IF @UseAltDataloc = 2 and ((@DBLooper % 2) = 0) SET @currDataloc = @Dataloc
	IF @UseAltDataloc = 2 and ((@DBLooper % 2) <> 0) SET @currDataloc = @Dataloc2


	print '-- Current Data Loc value is: ' + @currDataloc
	print char(9) + ' '
	print char(9) + ' '
	-- End of Set current data base Data files location


	-- Construct restore database statement	
	print char(9) + '-- Database to restore is: ' +  @DBTarget
	print char(9) + ' '
IF @BKType = 'D'
	BEGIN
	print char(9) + char(9) + 'IF NOT EXISTS (select name from sys.databases where name = ''' + @DBTarget + ''') '
	print char(9) + char(9) + 'BEGIN'
	END
	print char(9) + char(9) + char(9) + 'RESTORE DATABASE [' + @DBTarget +']'
	print char(9) + char(9) + char(9) + '-- RESTORE FILELISTONLY'
	print char(9) + char(9) + char(9) +'FROM DISK = ''' +  replace(@bkname, '\\ks-dd3128.amer.epiqcorp.com', '\\10.15.238.11\sql2') + ''''

if @FirstFam <> @LastFam 
	BEGIN
	DECLARE @BKFiles int 
	DECLARE @bknameX VARCHAR(200)
	DECLARE @DBFamLooper INT
	SET @BKFiles = @LastFam - @FirstFam
	SET @DBFamLooper = 2
	while @DBFamLooper < @LastFam + 1
		BEGIN
			SELECT @bknameX = m.physical_device_name
				--, m.media_set_id
				--, s.first_family_number
				--, s.last_family_number
				--, m.family_sequence_number
			FROM msdb.dbo.backupSET s
			INNER JOIN msdb.dbo.backupmediafamily m
				ON s.media_SET_id = m.media_SET_id
			WHERE --s.database_name =  @DBTarget AND
			 m.media_set_id = @MediaID and 
			 m.family_sequence_number = @DBFamLooper
			 --AND m.family_sequence_number > 1

		--, @MediaID = m.media_set_id
		--, @FirstFam = s.first_family_number
		--, @LastFam = s.last_family_number

			print char(9) + char(9) + char(9) +', DISK = ''' +  replace(@bknameX, '\\ks-dd3128.amer.epiqcorp.com', '\\10.15.238.11\sql2') + ''''
			set @DBFamLooper = @DBFamLooper + 1
		END
	END



-- Gather Physical Files Info within the creation of restore statement.
DECLARE @vDB varchar(100)
DECLARE	@vID INT
DECLARE	@vLF varchar(100)
DECLARE	@vFN varchar(300)
DECLARE @DBFilesLooper INT
DECLARE @curDB varchar(100)
DECLARE @DBFList table
		(DatabaseName varchar(100)
		, File_id Int
		, Logical_Name varchar(100)
		, File_name varchar(100)
		, Physical_name varchar(300)
		)
insert into @DBFList 
select * from ##vDatabaseFiles
where DatabaseName =  @DBTarget 

SET @DBFilesLooper = @@ROWCount
while @DBFilesLooper > 0
begin

	SET @vDB = ''
	SET @vID = 0
	SET @vLF = ''
	SET @vFN = ''
	SELECT TOP 1 
		@vID = File_id
		, @vLF = Logical_Name
		, @vFN = File_name
	FROM @DBFList


	If @vID = 1
		print char(9) + char(9) + char(9) +'WITH MOVE ''' + @vLF + ''' TO ''' + @currDataloc + @vFN + ''','
	If @vID = 2
		print char(9) + char(9) + char(9) +'MOVE ''' + @vLF + ''' TO ''' + @Logloc + @vFN + ''','
	If @vID >= 3
		BEGIN
			IF substring(@vLF,1,5) = 'ftrow'
				BEGIN
					--Print substring(@vLF,1,5) 
					set @currDataloc = @DatalocFTI
				END
			else
				BEGIN
					--print substring(@vLF,(len(@vLF)-3),4)
					DECLARE @currvLF varchar(2)
					set @currvLF = substring(@vLF,(len(@vLF)-1),2)
					set @currDataloc = 
						(CASE @currvLF
							WHEN '02' then @Dataloc2
							WHEN '03' then @Dataloc3
							WHEN '04' then @Dataloc4
							WHEN '05' then @Dataloc5
							WHEN '06' then @Dataloc6
							WHEN '07' then @Dataloc7
							WHEN '08' then @Dataloc8
							WHEN '09' then @Dataloc9
							WHEN '10' then @Dataloc10
							ELSE @currDataloc
						END
						)
				END
			print char(9) + char(9) + char(9) +'MOVE ''' + @vLF + ''' TO ''' + @currDataloc + @vFN + ''','

		END
	delete from @DBFList
	where File_id = @vID

	SET @DBFilesLooper = @DBFilesLooper - 1
end 
--drop table ##vDatabaseFiles
-- Gather Physical Files Info within the creation of restore statement.

IF @BKType = 'D'
	print char(9) + char(9) + char(9) +'NORECOVERY,'
ELSE
	print char(9) + char(9) + char(9) +'RECOVERY,'

	print char(9) + char(9) + char(9) +'STATS = 5'

IF @BKType = 'D'
	print char(9) + char(9) + 'END'

	print char(9) + ' '

	--SET @Script = ''

IF @BKType = 'I'  -- Restore wrap-up
	Begin
		print char(9) + char(9) + char(9) +'GO'
		print char(9) + char(9) + char(9) +'USE [' + @DBTarget + ']'
		print char(9) + char(9) + char(9) +'GO'
		print char(9) + char(9) + char(9) +'exec sp_changedbowner ''sa'''
		print char(9) + char(9) + char(9) +'ALTER DATABASE [' + @DBTarget + '] SET RECOVERY FULL'
		print char(9) + char(9) + char(9) +'EXEC sp_change_users_login ''report'''
		print char(9) + char(9) + char(9) +'-- EXEC sp_change_users_login ''Update_One'', ''user'', ''user'''
		--print char(9) + char(9) + char(9) +'EXEC sp_change_users_login ''report'''
		print char(9) + char(9) + char(9) +'GO'
		print char(9)
	End

	delete from @DatabaseList
	where DatabaseName = @DBTarget

	SET @DBLooper = @DBLooper - 1
end 

"

    $strsql > "C:\users\mjackson\temp\garytest.sql"

    Invoke-SqlCmd -ServerInstance $instance -Database 'msdb' -Query $strsql   -verbose 4> $outfileAmer    # "$basedir\$basefile_output.txt"   
}


FUNCTION XFER {
param (
  #[string]$username,
  #[string]$password,
  [string]$localFile,
  [string]$remoteFile
 )
Write-Host "Uploading"

function FileProgress
{
  Param($e)
  if ($e.FileProgress -ne $Null)
  {
    $transferSpeed = 0
    if ($e.CPS -ne $Null)
    {$transferSpeed = $e.CPS}
    $filePercent = $e.FileProgress*100
    
    Write-Progress -activity "FTP Upload" -status ("Uploading " + $e.FileName) -percentComplete ($filePercent) -CurrentOperation ("Completed: " + $filePercent + "% @ " + [Math]::Round(($transferSpeed/1024),2) + " k/bytes per second")
  }
}

try
{

#$cred = new-object -typename system.management.automation.PSCredential -argumentList $(whoami), ($passworrd | convertTo-SecureString)
#get-credential -credential $(whoami)
    
    $username = 'ward_' + $env:USERNAME
    $username = $username -replace " ",""
    $SecurePassword = read-host -prompt "Enter your USCUST Password please" -AsSecureString
    $BSTR = `
        [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecurePassword)
        $PlainPassword = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR) 

    # Load WinSCP .NET assembly
    # Use "winscp.dll" for the releases before the latest beta version.
    [Reflection.Assembly]::LoadFrom("C:\Program Files (x86)\WINSCP\winSCPnet.dll") | Out-Null
 
    # Setup session options
    $sessionOptions = New-Object WinSCP.SessionOptions
    $sessionOptions.Protocol = [WinSCP.Protocol]::sftp
    $sessionOptions.HostName = "10.11.180.11"
    $sessionOptions.UserName = $username
    $sessionOptions.Password =  $Plainpassword
    $SessionOptions.GiveUpSecurityAndAcceptAnySshHostKey
    $sessionOptions.SshHostKeyFingerprint = "ssh-rsa 1024 20:dd:23:50:8d:69:23:9c:bd:2a:c3:18:91:fb:42:80"  #sftp to host and get this
    $session = New-Object WinSCP.Session
 
    try
    {
    
        $session.add_FileTransferProgress( { FileProgress($_) } )
        
        # Connect
        $session.Open($sessionOptions)
        $remotePath = "/SQL Dba Team/ECA_MIG/"
        $filestomove = $localfile -split ' '
        
        foreach ($line in $filestomove)
        {
            write-host ("Uploading {0}..." -f $line)
            $session.PutFiles($line, $remotepath).check()

        }

    }
    finally
    {
        # Disconnect, clean up
        $session.Dispose()
    }
 
    exit 0
}
catch [Exception]
{
    Write-Host $_.Exception.Message
   # exit 1
}
}

Function SelectDestination #select the destination and data drive to restore to
 {
  
 #$instance = "sql02"
    $objForm = New-Object System.Windows.Forms.Form 
    $objForm.Text = "restore to:"
    $objForm.Size = New-Object System.Drawing.Size(300,200) 
    $objForm.StartPosition = "CenterScreen"

    $objForm.KeyPreview = $True
    $objForm.Add_KeyDown({if ($_.KeyCode -eq "Enter") 
        {$x=$objListBox.SelectedItem;$objForm.Close()}})
    $objForm.Add_KeyDown({if ($_.KeyCode -eq "Escape") 
        {$objForm.Close()}})

    $OKButton = New-Object System.Windows.Forms.Button
    $OKButton.Location = New-Object System.Drawing.Size(75,120)
    $OKButton.Size = New-Object System.Drawing.Size(75,23)
    $OKButton.Text = "OK"
    $OKButton.Add_Click({$x=$objListBox.SelectedItem;$objForm.Close()})
    $objForm.Controls.Add($OKButton)

    $CancelButton = New-Object System.Windows.Forms.Button
    $CancelButton.Location = New-Object System.Drawing.Size(150,120)
    $CancelButton.Size = New-Object System.Drawing.Size(75,23)
    $CancelButton.Text = "Cancel"
    $CancelButton.Add_Click({$objForm.Close()})
    $objForm.Controls.Add($CancelButton)

    $objLabel = New-Object System.Windows.Forms.Label
    $objLabel.Location = New-Object System.Drawing.Size(10,20) 
    $objLabel.Size = New-Object System.Drawing.Size(280,20) 
    $objLabel.Text = "Select instance & Drive to restore TO:"
    $objForm.Controls.Add($objLabel) 

    $objListBox = New-Object System.Windows.Forms.ListBox 
    $objListBox.Location = New-Object System.Drawing.Size(10,40) 
    $objListBox.Size = New-Object System.Drawing.Size(260,20) 
    $objListBox.Height = 80

    #these are prod values - 
    #if($instance -eq "P016sqlc0102\sql02")

    #{
    [void] $objListBox.Items.Add("ECA02 F:\SQL02_DATA_MP01\MSSQL\DATA\")
    [void] $objListBox.Items.Add("ECA02 F:\SQL02_DATA_MP02\MSSQL\DATA\")
    [void] $objListBox.Items.Add("ECA02 F:\SQL02_DATA_MP03\MSSQL\DATA\")
    [void] $objListBox.Items.Add("ECA02 F:\SQL02_DATA_MP04\MSSQL\DATA\")
    #}
    #if ($instance -eq "P016sqlc0101\sql01") {
    [void] $objListBox.Items.Add("ECA01 E:\SQL01_DATA_MP01\MSSQL\DATA\")
    [void] $objListBox.Items.Add("ECA01 E:\SQL01_DATA_MP02\MSSQL\DATA\")
    #}

    $objForm.Controls.Add($objListBox) 

    $objForm.Topmost = $True

    $objForm.Add_Shown({$objForm.Activate()})
    [void] $objForm.ShowDialog()
    $DestinationInstance = $objListBox.SelectedItem
    write-host "inside function here is the selected item:" $destinationInstance
    return $DestinationInstance
    #write-host $SourceInstance
  }


Function Get-FileName($initialDirectory)
{
    [System.Reflection.Assembly]::LoadWithPartialName("System.windows.forms") | Out-Null
    
    $OpenFileDialog = New-Object System.Windows.Forms.OpenFileDialog
    $OpenFileDialog.initialDirectory = $initialDirectory
    #$OpenFileDialog.filter = "CSV (*.csv)| *.csv"
    $OpenFileDialog.ShowDialog() | Out-Null
    $OpenFileDialog.filename
}


Function SelectSource #will give list box with instance selection
 {
    $objForm = New-Object System.Windows.Forms.Form 
    $objForm.Text = "Instance list:"
    $objForm.Size = New-Object System.Drawing.Size(300,200) 
    $objForm.StartPosition = "CenterScreen"

    $objForm.KeyPreview = $True
    $objForm.Add_KeyDown({if ($_.KeyCode -eq "Enter") 
        {$x=$objListBox.SelectedItem;$objForm.Close()}})
    $objForm.Add_KeyDown({if ($_.KeyCode -eq "Escape") 
        {$objForm.Close()}})

    $OKButton = New-Object System.Windows.Forms.Button
    $OKButton.Location = New-Object System.Drawing.Size(75,120)
    $OKButton.Size = New-Object System.Drawing.Size(75,23)
    $OKButton.Text = "OK"
    $OKButton.Add_Click({$x=$objListBox.SelectedItem;$objForm.Close()})
    $objForm.Controls.Add($OKButton)

    $CancelButton = New-Object System.Windows.Forms.Button
    $CancelButton.Location = New-Object System.Drawing.Size(150,120)
    $CancelButton.Size = New-Object System.Drawing.Size(75,23)
    $CancelButton.Text = "Cancel"
    $CancelButton.Add_Click({$objForm.Close()})
    $objForm.Controls.Add($CancelButton)

    $objLabel = New-Object System.Windows.Forms.Label
    $objLabel.Location = New-Object System.Drawing.Size(10,20) 
    $objLabel.Size = New-Object System.Drawing.Size(280,20) 
    $objLabel.Text = "Select instance to bakcup FROM:"
    $objForm.Controls.Add($objLabel) 

    $objListBox = New-Object System.Windows.Forms.ListBox 
    $objListBox.Location = New-Object System.Drawing.Size(10,40) 
    $objListBox.Size = New-Object System.Drawing.Size(260,20) 
    $objListBox.Height = 80

    #these are prod values - 
    [void] $objListBox.Items.Add("P016SQLC0101\SQL01")
    [void] $objListBox.Items.Add("P016SQLC0102\SQL02")

    $objForm.Controls.Add($objListBox) 

    $objForm.Topmost = $True

    $objForm.Add_Shown({$objForm.Activate()})
    [void] $objForm.ShowDialog()
    $SourceInstance = $objListBox.SelectedItem
    return $sourceInstance
    #write-host $SourceInstance
  }


#################               MAIN                     ##############################
#gather what is needed to read the txt file of db's to operate on and which instance
TRY
{
 $instance = selectsource  
 $filetouse = Get-FileName 'c:\users\mjackson\temp'
 #write-host "Here is the file you selected" $filetouse
 $DestinationInstance = SelectDestination
 write-host $DestinationInstance  " that is the location to restore to"
 $destinationArr =$DestinationInstance -split ' '
 $dstinstance = $destinationArr[0]
 $datafilelocation = $destinationArr[1]
 write-host "here is destination instance" $dstinstance " and the path " $datafilelocation

 
 #write-host "Here is the source instance and destination filelocation" $instance  $datafilelocation
 #parse out server name and datafile location 
 #  $targetdrive = $i %{$data = $_.split(" "); Write-Output "$($data[0])
 #prompt to ask if they want to sftp file

 #now set the filepath location so that bulk insert will work.  file needs to reside on the instance in amer.
 switch ($instance)
    {
        "P016sqlc0101\sql01" {$serverpath = '\\P016sqlc0101\d$\migration'}  ##where the txt file stored that contains dbs to backup/restore so we can bulk load
        "P016sqlc0102\sql02" {$serverpath = '\\P016sqlc0102\E$\Migration'}  ##where the txt file stored that contains dbs to backup/restore so we can bulk load
        "P016sqlc0101\sql01" {$logdir = 'E:\SQL01_LOGS_MP01\MSSQL\Logs\'}   ##Instance log location 
        "P016sqlc0102\sql02" {$logdir = 'F:\SQL02_Logs_MP01\MSSQL\Logs\'}   ##Instance log location 
    }
 if(test-path filesystem::$filetouse) {
        write-host "File of databases to work on exists, will proceed"
        copy-item -Path $filetouse -Destination  filesystem::$serverpath  #copy it to the server so bulk works.
        $basefile =  [io.path]::getfilenamewithoutextension($filetouse)  #get just the filename.
        $basedir = [io.path]::GetDirectoryName($filetouse)   #get base dir user specified
        $bulkfile = $serverpath +'\' + $basefile + ".txt"    #what will be placed on the sql instance
        $sqloutfile = $basedir + '\' + $basefile + "outfile.sql" #will be what we move over to vegas to restore from
        $outfileamer = $basedir +'\' + $basefile +"outfile.txt"
      #  write-host "here is bulkfile " $bulkfile
    } 
 else 
    {
        write-host "file not found, try again"
        return
    }      
    #test with C:\users\mjackson\temp\migration_test.txt
  #  $strsql > "C:\users\mjackson\temp\garytest.sql"


  #  Invoke-SqlCmd -ServerInstance $instance -Database 'msdb' -Query $strsql   -verbose 4> $outfileAmer    # "$basedir\$basefile_output.txt"   
 Run_sql $datafilelocation $bulkfile $logdir
    #strip out the context switch statement(s)...verbose tosses those in for fun.
    Get-Content $outfileAmer | ? {$_ -notmatch 'Changed database context to'} | ? {$_ -notmatch 'Bulk load:'} | Set-Content $sqloutfile


  #set variable to files to xfer
    #if(test-path "$sqloutfile" -and test-path "$somefilehere")

    #now test for outfile existing and sftp it over  - full should already be there.
    if(test-path "$sqloutfile" ) 
      {
        write-host 'Will start sftp now'
        xfer "$sqloutfile" "/SQL Dba Team/ECA_MIG/"
      }
 }
 catch [Exception]
  {
    Write-Host $_.Exception.Message
   # exi
  }
  finally
  {
   write-host "done"
   }
  exit 0
    
 
