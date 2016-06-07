<# Will read in a file of databases and take that and generate a restore script then sftp it over to vegas.
script it generates will restore fulls but without recovery, so they will be awaiting the diff.  Please note, there is NO
automatino as to where the files will go on the new prod instance, you will still need to comment / un-comment the sql below in strsql variable
#this is a test.

#>

function run_sql {


 [CmdletBinding()] 
    [Parameter(Mandatory=$true)] [string]$bulkfile,
    [Parameter(Mandatory=$true)] [string]$logdir
        
$strsql="
use msdb
declare @DatabaseList table (DatabaseName varchar(100))
declare @DatabaseBackups table (DatabaseName varchar(100), BackupType char(4), FileLocation varchar(500), [Rank] int)
declare @DatabaseFiles table (DatabaseName varchar(100), BackupType char(4), FileType varchar(4), LogicalName varchar(100), [FileName] varchar(100), [Rank] int)
declare @Script varchar(max),
		@script2 varchar(max),
        @script3 varchar(max),
		@Looper int,
		@DatabaseTarget varchar(100),
		@DatafileDestination varchar(500),
		@LogfileDestination varchar(500),
	    @PQgroupname VARCHAR(100),   --this is the powerquery group we will no longer need  = 'amer\ISusrP-' + DB_NAME() + '_Acct_Mgr'
		@PQPoormanGroupname varchar(100)

set nocount on

set @Script =  ''
set @script2 = ''
set @script3 = ''

if exists (select * from INFORMATION_SCHEMA.Tables where table_name = '#tmpdbnames')
	drop table #tmpdbnames;

create table #tmpDBnames (
	dbname varchar(250)
	)
bulk insert #tmpdbnames FROM '$bulkfile'
with (ROWTERMINATOR = '\n')


insert into @DatabaseList (DatabaseName)
select [DatabaseName] = name
from sys.databases
where name <> N'master'
  and name <> N'tempdb'
  and name <> N'model'
  and name <> N'msdb'
  and name <> N'ConfigurationDatabase'
  and name <> N'DBASupport'
  and name <> N'Reporting'
  and name in (select dbname from #tmpdbnames)
-- and (name = 'CA_Greeno_vs_NationwideInsurance' or name = 'CA0123' or name = 'CA0418')
select @Looper = @@rowcount

insert into @DatabaseBackups (DatabaseName, BackupType, FileLocation, [Rank])
select
	[DatabaseName] = bs.database_name,
	[BackupType] = case when bs.type = N'D' then 'Full'
	                    when bs.type = N'I' then 'Diff' end,
	[FileLocation] = replace(bmf.physical_device_name, '\\ks-dd3128.amer.epiqcorp.com\sql', '\\10.15.238.11\sql2\sql'),
	--[FileLocation] = replace(bmf.physical_device_name, '\\ks-dd3128.amer.epiqcorp.com\sql', '\\10.15.238.11\sql2\sql'),
	[Rank] = rank() over (partition by bs.database_name, bs.type order by bs.backup_finish_date desc)
from backupset bs
join backupmediafamily bmf on bs.media_set_id = bmf.media_set_id
where bs.database_name in (select DatabaseName from @DatabaseList)
and (bs.type = N'D' or bs.type = N'I')
and bs.backup_finish_date >= getutcdate()-8
--and (bs.database_name  = 'CA8233' or bs.database_name =  'CA8244')

insert into @DatabaseFiles (DatabaseName, BackupType, FileType, LogicalName, [FileName], [Rank])
select
	[DatabaseName] = bs.database_name,
	[BackupType] = case when bs.type = N'D' then 'Full'
	                    when bs.type = N'I' then 'Diff' end,
	[FileType] = case when bf.file_type = 'D' then 'Data'
					  when bf.file_type = 'L' then 'Log' end,
	[LogicalName] = bf.logical_name,
	[FileName] = right(bf.physical_name, charindex('\', reverse(bf.physical_name)) - 1),
	[Rank] = rank() over (partition by bs.database_name, bs.type order by bs.backup_finish_date desc)
from backupset bs
join backupfile bf on bs.backup_set_id = bf.backup_set_id
where bs.database_name in (select DatabaseName from @DatabaseList)
and (bs.type = N'I')
and bs.backup_finish_date >= getutcdate()-8

declare @final varchar(max) = ''
while @Looper > 0

begin

set @DatabaseTarget = ''
declare @fileloc varchar(300)
--set @fileloc = 'D:\Restore_testing_backups\'


select top 1 @DatabaseTarget = DatabaseName
from @DatabaseList

select @Script = @Script + 'use [master]
GO
declare @exists bit
select @exists =  count(*) from sys.databases where name = ''' + dl.databasename + ''' and state_desc = ''online''


IF @exists = 0
BEGIN
	RESTORE DATABASE [' + dl.DatabaseName + '] FROM DISK = N''' + db.filelocation +''' WITH FILE = 1,'
	from @DatabaseList dl
	join @DatabaseBackups db on dl.DatabaseName = db.DatabaseName
	join @DatabaseFiles df on df.DatabaseName = db.DatabaseName and db.BackupType = df.BackupType
	where db.[Rank] = 1
	  and df.[Rank] = 1
	  and db.BackupType = 'Diff'
	  and dl.DatabaseName = @DatabaseTarget
	group by dl.DatabaseName, db.FileLocation

	/*
	select @Script = @Script + ' 
	MOVE N''' + df.LogicalName + ''' TO N''' + @DatafileDestination + df.[FileName] + ''','
	from @DatabaseList dl
	join @DatabaseBackups db on dl.DatabaseName = db.DatabaseName
	join @DatabaseFiles df on df.DatabaseName = db.DatabaseName and db.BackupType = df.BackupType
	where db.[Rank] = 1
	  and df.[Rank] = 1
	  and db.BackupType = 'Diff'
	  and df.FileType = 'Data'
	  and dl.DatabaseName = @DatabaseTarget

	select @Script = @Script + ' 
	MOVE N''' + df.LogicalName + ''' TO N''' + @LogfileDestination + df.[FileName] + ''','
	from @DatabaseList dl
	join @DatabaseBackups db on dl.DatabaseName = db.DatabaseName
	join @DatabaseFiles df on df.DatabaseName = db.DatabaseName and db.BackupType = df.BackupType
	where db.[Rank] = 1
	  and df.[Rank] = 1
	  and db.BackupType = 'Diff'
	  and df.FileType = 'Log'
	  and dl.DatabaseName = @DatabaseTarget

  */

	 SET  @PQgroupname  = 'amer\ISusrP-' + @databasetarget + '_Acct_Mgr'
	 SET @PQPoormanGroupname = 'poorman-douglas\ISusrp-'+  @databasetarget + '_Acct_Mgr'
	-- print @pqgroupname
set @script2 = @script2 + char(10) + ' DECLARE @sql VARCHAR(max) 
	  set @sql = ''
	alter database ' + @databaseTarget + ' set COMPATIBILITY_LEVEL = 120
	  USE ' + @databaseTarget + '  
	  exec sp_changedbowner  ''''sa''''

	  If Exists (select name from sys.all_objects where name = ''''RegenerateMasterKey'''')
	   BEGIN
		exec dbo.RegenerateMasterKey
	   END
	GRANT connect to [USCUST\GG-FIMRole USCUST ECA Data Services]
	GRANT connect to [USCUST\GG-FIMRole USCUST ECA Development]

	exec sp_addrolemember ''''db_datareader'''', [USCUST\GG-FIMRole USCUST ECA Data Services]
	exec sp_addrolemember ''''db_datareader'''', [USCUST\GG-FIMRole USCUST ECA Development]
	exec sp_addrolemember ''''db_datawriter'''', [USCUST\GG-FIMRole USCUST ECA Data Services]
	exec sp_addrolemember ''''db_datawriter'''', [USCUST\GG-FIMRole USCUST ECA Development]

	IF EXISTS (select name from sys.database_principals where name = ''''usrca'''')
	  BEGIN
		EXEC sp_change_users_login ''''update_one'''',''''usrca'''',''''usrca''''
	  END
	IF EXISTS (select name from sys.database_principals where name = ''''CA_Web_User'''')
	  BEGIN
		EXEC sp_change_users_login ''''update_one'''',''''CA_Web_User'''',''''CA_Web_User''''
	  END
	IF EXISTS (select name from sys.database_principals where name = ''''rptuser'''')
	  BEGIN
	   EXEC sp_change_users_login ''''update_one'''',''''rptuser'''',''''rptuser'''' 
	 END
	IF EXISTS (select name from sys.database_Principals where name = ''''OCR'''')
	 BEGIN
		EXEC sp_change_users_login ''''update_one'''', ''''OCR'''', ''''OCR''''
	 END
	IF EXISTS (select name from sys.database_principals where name = ''''bulk_nightly'''')
	  BEGIN
		EXEC sp_change_users_login ''''update_one'''', ''''bulk_nightly'''', ''''bulk_nightly''''
	  END
	if EXISTS (select name from sys.database_principals where name = ''''' + @pqGroupName + ''''')
	   BEGIN
			drop user [' + @PqGroupName + ']
	   END
	if EXISTS (select name from sys.database_principals where name = ''''' + @PQPoormanGroupname + ''''')
	   BEGIN
	   IF EXISTS (Select name from sys.schemas where name =  ''''' + @PQPoormanGroupname + ''''')
			BEGIN
			  ALTER AUTHORIZATION ON SCHEMA::['+ @PQPoormanGroupname + '] TO dbo
			  drop schema [' + @PQPoormanGroupname + ']
				END
		drop user [' + @PQPoormanGroupname + ']
	    END
	IF EXISTS (select name from sys.schemas where name = ''''tmpprod'''')
		BEGIN
			DROP SCHEMA [tmpprod]
		END

	   
	if EXISTS (select name from sys.database_principals where name = ''''AMER\ET-GG-ECA_PWRQRY'''')
	   BEGIN
			drop user [AMER\ET-GG-ECA_PWRQRY]
	   END

	IF EXISTS (select name from sys.database_principals where name = ''''tmpProd'''')
	   BEGIN
			drop user [tmpProd]
	   END

	IF EXISTS (select name from sys.database_principals where name = ''''ECA_DataAnalysts'''')
	   BEGIN
			DROP USER [ECA_DataAnalysts]
	   END

	IF EXISTS (select name from sys.database_principals where name = ''''AMER\OCR'''')
		BEGIN
			IF EXISTS (select name from sys.schemas where name = ''''AMER\OCR'''')
			BEGIN
				DROP SCHEMA [AMER\OCR]
			END
		DROP USER [AMER\OCR]
	   END 

	 IF EXISTS (select name from sys.database_principals where name = ''''Amer\svc_docprod'''')
	   BEGIN
			grant connect to [USCUST\svc_DocProd_prod]
			exec sp_addrolemember ''''roleimageviewer'''', ''''USCUST\svc_DocProd_prod'''' 
		     If EXISTS (select name from sys.schemas where name = ''''amer\svc_docprod'''')
			    BEGIN
				  ALTER AUTHORIZATION ON SCHEMA::[Amer\svc_docprod] TO [USCUST\svc_DocProd_prod]
				  DROP SCHEMA [amer\svc_docprod]
				  END
			DROP USER [amer\svc_docprod]
	   END


	IF EXISTS (select name from sys.database_principals where name = ''''AMER\SVC_CLaimsMatrixWeb'''')
	  BEGIN
		  grant connect to [USCUST\svc_CMWeb_prod]
				IF EXISTS (select name from sys.schemas where name = ''''AMER\svc_ClaimsMatrixWeb'''')
					BEGIN
						ALTER AUTHORIZATION ON SCHEMA::[Amer\SVC_CLaimsMatrixWeb] TO [USCUST\svc_CMWeb_prod]
						DROP SCHEMA [AMER\SVC_CLaimsMatrixWeb]
					END
			exec sp_addrolemember ''''roleCAuser'''', ''''USCUST\svc_CMweb_prod''''
			DROP USER [AMER\SVC_CLaimsMatrixWeb]
	  END

	IF EXISTS (select table_name from information_schema.tables where table_name = ''''documentstaging''''
				and table_schema = ''''dbo'''')
	   BEGIN
			grant select on dbo.documentstaging to rolecauser
		END



	IF EXISTS (select name from sys.database_principals where name = ''''POORMAN-DOUGLAS\lexprocess'''')
	    BEGIN
			grant connect to [USCUST\svc_LexProcess_prod]
				IF EXISTS (select name from sys.schemas where name = ''''POORMAN-DOUGLAS\lexprocess'''')
				   BEGIN
					ALTER AUTHORIZATION ON SCHEMA::[POORMAN-DOUGLAS\lexprocess] TO [USCUST\svc_LexProcess_prod]
					DROP SCHEMA [POORMAN-DOUGLAS\lexprocess]
				   END
			exec sp_addrolemember ''''roleimageviewer'''', ''''USCUST\svc_LexProcess_prod'''' 			
			DROP USER [POORMAN-DOUGLAS\lexprocess]
	     END

	IF EXISTS (select name from sys.database_principals where name = ''''AMER\CA_productionDBsupport'''')
		BEGIN
			IF EXISTS (select name from sys.schemas where name = ''''AMER\CA_ProductionDBSupport'''')
			BEGIN
				ALTER AUTHORIZATION ON SCHEMA::[AMER\CA_ProductionDBSupport] to DBO
								DROP SCHEMA [AMER\CA_ProductionDBsupport]
			END
		DROP USER [AMER\CA_productionDBsupport]
	   END

	IF EXISTS (select name from sys.database_principals where name = ''''CA_productionDBsupport'''')
		BEGIN
			IF EXISTS (select name from sys.schemas where name = ''''CA_ProductionDBSupport'''')
			BEGIN
				DROP SCHEMA [CA_ProductionDBsupport]
			END
		DROP USER [CA_productionDBsupport]
	   END


	IF EXISTS (select name from sys.database_principals where name = ''''AMER\CA_ApplicationDev'''')
		BEGIN
			IF EXISTS (select name from sys.schemas where name = ''''AMER\CA_ApplicationDev'''')
				BEGIN
					ALTER AUTHORIZATION ON SCHEMA::[AMER\CA_ApplicationDev] to dbo
					DROP SCHEMA [AMER\CA_ApplicationDev]
				END
		 DROP USER [AMER\CA_ApplicationDev]
		END


	IF EXISTS (select name from sys.database_principals where name = ''''AMER\ECA_SoftwareEngineering'''')
		BEGIN
			IF EXISTS (select name from sys.schemas where name = ''''AMER\ECA_SoftwareEngineering'''')
			BEGIN
				DROP SCHEMA [AMER\ECA_SoftwareEngineering]
			END
		DROP USER [AMER\ECA_SoftwareEngineering]
	   END
'
Set @script3 = @script3 + Char(10) +
'
	IF EXISTS (select name from sys.database_principals where name = ''''AMER\ET-GG-ECA_READONLY'''')
		BEGIN
			IF EXISTS (select name from sys.schemas where name = ''''AMER\ET-GG-ECA_READONLY'''')
			BEGIN
				DROP SCHEMA [AMER\ET-GG-ECA_READONLY]
			END
		DROP USER [AMER\ET-GG-ECA_READONLY]
	   END

	IF EXISTS (select name from sys.database_principals where name = ''''AMER\CA_BusinessAnalysts_ReadOnly'''')
		BEGIN
			IF EXISTS (select name from sys.schemas where name = ''''AMER\CA_BusinessAnalysts_ReadOnly'''')
			BEGIN
				DROP SCHEMA [AMER\CA_BusinessAnalysts_ReadOnly]
			END
		DROP USER [AMER\CA_BusinessAnalysts_ReadOnly]
	   END
	   
	 IF EXISTS (select name from sys.database_principals where name = ''''POORMAN-DOUGLAS\spSchedTasks'''')
		BEGIN
			IF EXISTS (select name from sys.schemas where name = ''''POORMAN-DOUGLAS\spSchedTasks'''')
			BEGIN
				DROP SCHEMA [POORMAN-DOUGLAS\spSchedTasks]
			END
		DROP USER [POORMAN-DOUGLAS\spSchedTasks]
	   END
	    
	 IF EXISTS (select name from sys.database_principals where name = ''''AMER\CA-ETLMatrix-User'''')
		BEGIN
			IF EXISTS (select name from sys.schemas where name = ''''AMER\CA-ETLMatrix-User'''')
			BEGIN
				DROP SCHEMA [AMER\CA-ETLMatrix-User]
			END
		DROP USER [AMER\CA-ETLMatrix-User]
	 END
	 IF EXISTS (select name from sys.database_principals where name = ''''POORMAN-DOUGLAS\CA_BusinessAnalysts'''')
		BEGIN
			IF EXISTS (select name from sys.schemas where name = ''''POORMAN-DOUGLAS\CA_BusinessAnalysts'''')
			BEGIN
				DROP SCHEMA [POORMAN-DOUGLAS\CA_BusinessAnalysts]
			END
		DROP USER [POORMAN-DOUGLAS\CA_BusinessAnalysts]
	   END
	 IF EXISTS (select name from sys.database_principals where name = ''''POORMAN-DOUGLAS\CA_QualityAssurances'''')
		BEGIN
			IF EXISTS (select name from sys.schemas where name = ''''POORMAN-DOUGLAS\CA_QualityAssurance'''')
			BEGIN
				DROP SCHEMA [POORMAN-DOUGLAS\CA_QualityAssurance]
			END
		DROP USER [POORMAN-DOUGLAS\CA_QualityAssurance]
	 END
	 IF EXISTS (select name from sys.database_principals where name = ''''Poorman-Douglas\DocProd'''')
		BEGIN
			IF EXISTS (select name from sys.schemas where name = ''''Poorman-Douglas\DocProd'''')
			BEGIN
				DROP SCHEMA [Poorman-Douglas\DocProd]
			END
		DROP USER [Poorman-Douglas\DocProd]
	 END

	
	 IF EXISTS (select name from sys.database_principals where name = ''''Amer\svc_docprod'''')
	   BEGIN
			grant connect to [USCUST\svc_DocProd_prod]
			exec sp_addrolemember ''''roleimageviewer'''', ''''USCUST\svc_DocProd_prod'''' 
		     If EXISTS (select name from sys.schemas where name = ''''amer\svc_docprod'''')
			    BEGIN
				  ALTER AUTHORIZATION ON SCHEMA::[Amer\svc_docprod] TO [USCUST\svc_DocProd_prod]
				  DROP SCHEMA [amer\svc_docprod]
				  END
			DROP USER [amer\svc_docprod]
	   END
	   ''
	EXEC sp_sqlexec @Sql

END
ELSE

print ''' + @databaseTarget + ''' + '' ALREADY EXISTSTS ON THIS INSTANCE - REVIEW SCHEDULE''

'
select @script = @script + ' RECOVERY, NOUNLOAD, STATS = 5'
delete from @DatabaseList
where DatabaseName = @DatabaseTarget


set @Looper = @Looper - 1

--print @Script
--note - print statement limited to 8k, thus we have to output xml - this will have tags that need to be removed:  <row>  and </row>
set @final = @script 

Print @script 
print @script2
print @script3

set @Script = ''
set @script2 = ''
set @script3 = ''

end
--select @final for xml PATH
drop table #tmpDBnames

"

    $strsql > "C:\users\mjackson\temp\strsqltest.sql"

    invoke-sqlcmd  -ServerInstance $instance -database 'msdb' -query $strsql -verbose 4> $outfileAmer

   # Invoke-SqlCmd -ServerInstance $instance -Database 'msdb' -Query $strsql   -verbose 4> $outfileAmer    # "$basedir\$basefile_output.txt"   
   # sqlcmd -S $instance -d 'msdb' -q $strsql 
    
}



FUNCTION XFER {  #will do the sftp
param 
  (
  [string]$filestoSFTP,
  [string]$remoteFile
  )
add-type -Path "C:\Program Files (x86)\WINSCP\winSCPnet.dll"  #winscp dll used to sftp.
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

   # Load WinSCP .NET assembly
    # Use "winscp.dll" for the releases before the latest beta version.
    [Reflection.Assembly]::LoadFrom("C:\Program Files (x86)\WINSCP\winSCPnet.dll") | Out-Null
    $username = 'ward_' + $env:USERNAME
    $username = $username -replace " ",""
    $SecurePassword = read-host -prompt "Enter your USCUST Password please" -AsSecureString
    $BSTR = `
        [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecurePassword)
        $PlainPassword = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR) 
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
        $filestomove = $filestosftp -split ' '
        

        foreach ($line in $filestomove)
        {
            write-host ("Uploading {0}..." -f $line)
            $session.PutFiles($line, $remotepath).check()

        }
    
      <#  $session.add_FileTransferProgress( { FileProgress($_) } )
        
        # Connect
        $session.Open($sessionOptions)
 
        # Upload files
        $transferOptions = New-Object WinSCP.TransferOptions
        $transferOptions.TransferMode = [WinSCP.TransferMode]::Binary
 
        $transferResult = $session.PutFiles($localfile, $remoteFile, $False, $transferOptions)
 
        # Throw on any error
        $transferResult.Check()
 
        # Print results
        foreach ($transfer in $transferResult.Transfers)
        {
            Write-Host ("Upload of {0} succeeded" -f $transfer.FileName)
        }
       #>
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
}  #Do the sftp here to vegas
##this takes two variables - input filepath/name and output file/path name.  Input file should be db's we are migrating.

Function OBTAIN_BACKUPNAMES
{
  [CmdletBinding()]
    Param (
        [Parameter(Mandatory=$true)] [string] $filetouse,  #will be the list of db's we backed up txt file
       # [Parameter(mandatory=$false)] [string] $outfile,
        [Parameter(mandatory=$true)] [string] $instance,
        [Parameter(Mandatory=$true)] [string] $backuplistoutfile
          )
       # write-host "here is list " +   $dblist
  #now check to make sure the outfile does NOT exist, if so remove it.  
  #build the outfile name based off of the dblist param 
  #$outfile = (get-item Filesystem::"$dblist").directoryname + '\' + (get-item filesystem::$dblist).basename + "_output.txt"
  #$outfile
  IF (Test-Path filesystem::$backuplistoutfile)  #if the outfile exists whack it. 
    {  Remove-Item filesystem::$backuplistoutfile  
    }   
  $dbnames = get-content filesystem::"$filetouse"


  FOREACH ($dbname in $dbnames)
    {
      $strsql = "select distinct replace(z.physical_device_name, '\\ks-dd3128.amer.epiqcorp.com', '\\10.15.238.11\sql2') as physical_device_Name from (
                       select  database_name
	                   , rank() over (partition by bs.database_name order by bs.backup_finish_date desc) as bkup_rank
	                   , DATEDIFF(hh, bs.backup_finish_date, GETDATE()) as hoursago
	                   , bmf.physical_device_name
	                   from msdb..backupmediafamily as bmf--backup files
                       join msdb..backupset as bs on bmf.media_set_id = bs.media_set_id
                       where 
                       bs.type in ('I')       
	                   ) as z 
              where bkup_rank = 1 
              and z.database_name  in (select name from sys.databases)
              and hoursago < 48
              and database_name NOT in ('master', 'msdb', 'model')
              and database_name = '$dbname'"
     $filestocheck = @()
     $filestocheck = Invoke-Sqlcmd -serverinstance $instance -Database 'MSDB' -Query $strsql
     $filestocheck.physical_device_name
     $filestocheck.physical_device_name | out-file -append filesystem::"$backuplistoutfile"
    }
    
}  #end function

Function PULL_BACKUPS
{
  [CmdletBinding()]
    Param (
        [Parameter(Mandatory=$true)] [string] $filetouse,
        #[Parameter(mandatory=$true)] [string] $outfile,
        [Parameter(mandatory=$true)] [string] $instance,
        [Parameter(mandatory=$true)] [string] $backuplistoutfile
          )

#iterate thru the dblist and comma delimit for Hallengren
IF ($dbsTOBackup =  get-content filesystem::"$filetouse") 
  {
   
    foreach ($rec in $dbsTOBackup) 
    {
        # write-host $rec","
        $list += $rec  + ","
    
    }

$list = $list.substring(0,$list.length-1)  #strip last comma off.
"here is list : " + $list

    $backup_string = "EXECUTE [dbo].[DatabaseBackup] @Databases ='$list', @Directory = N'\\ks-dd3128.amer.epiqcorp.com\sql', @BackupType = 'DIFF', @ChangeBackupType='N', @CleanupTime = 268, @Compress = 'N', @LogToTable = 'Y' "
    # $backup_string

    #Invoke-SqlCmd -serverinstance $sqlinstance -Database "master" -Query $backup_string -QueryTimeout 0

        #once all this is done, call the function to generate the list of backup files that we will use to restore from - this list will be used in vegas to determin if all files have replicated over
    OBTAIN_BACKUPNAMES $filetouse  $instance $backuplistoutfile
  
  }
ELSE {
    write-host "file does not exist, please try again"
    break
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

    [void] $objListBox.Items.Add("ECA01") 
    [void] $objListBox.Items.Add("ECA02")


    $objForm.Controls.Add($objListBox) 

    $objForm.Topmost = $True

    $objForm.Add_Shown({$objForm.Activate()})
    [void] $objForm.ShowDialog()
    $DestinationInstance = $objListBox.SelectedItem
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
 $filetouse = Get-FileName 'c:\users\mjackson\temp'              #have user browse to file that will drive what db's we work on
 $DestinationInstance = SelectDestination
 #write-host $DestinationInstance  " that is the location to restore to"
 $backuplistoutfile = (get-item Filesystem::"$filetouse").directoryname + '\' + (get-item filesystem::$filetouse).basename  + "_backuplist.txt"


 ##now pull the backups (diff)
 #pull_backups "$filetouse" "$instance" "$backuplistoutfile"  #may still need define outfile

 ##if backups succeeded generate the list of backup filepath/files to check for DD replicated over to vegas.

 #now set the filepath location so that bulk insert will work.  file needs to reside on the SQL instance in AMER for best reliability
  switch ($instance)
    {
        "P016sqlc0101\sql01" {$serverpath = '\\P016sqlc0101\d$\migration'}  ##where the txt file stored that contains dbs to backup/restore so we can bulk load
        "P016sqlc0102\sql02" {$serverpath = '\\P016sqlc0102\E$\Migration'}  ##where the txt file stored that contains dbs to backup/restore so we can bulk load
        "P016sqlc0101\sql01" {$logdir = 'E:\SQL01_LOGS_MP01\MSSQL\Logs\'}   ##Instance log location 
        "P016sqlc0102\sql02" {$logdir = 'F:\SQL02_Logs_MP01\MSSQL\Logs\'}   ##Instance log location 
    }
  if(test-path filesystem::$filetouse) 
    {
        #write-host "File exists"
        copy-item -Path $filetouse -Destination  filesystem::$serverpath  #copy it to the server so bulk works.
        $basefile =  [io.path]::getfilenamewithoutextension($filetouse)  #get just the filename.
        $basedir = [io.path]::GetDirectoryName($filetouse)   #get base dir user specified
        $bulkfile = $serverpath +'\' + $basefile + ".txt"    #what will be placed on the sql instance
        $sqloutfile = $basedir + '\' + $basefile + "outfile_dIFF.sql" #will be what we move over to vegas to restore from
        $outfileamer = $basedir +'\' + $basefile +"outfile_DIFF.txt"
      #  write-host "here is bulkfile " $bulkfile
    } 
  else 
    {
        write-host "file not found, try again"
        return
    }      
    #test with C:\users\mjackson\temp\migration_test.txt
  #  $strsql > "C:\users\mjackson\temp\garytest.sql"

  #Now go ahead and pull backups - this will generate the txt file of backup names as well
  PULL_BACKUPS $filetouse $instance $backuplistoutfile

     #  call to function to generate restore script   
    Run_sql  $bulkfile $logdir
      #strip out the context switch statement(s)...verbose tosses those in for fun.
    Get-Content $outfileAmer | ? {$_ -notmatch 'Changed database context to'} | ? {$_ -notmatch 'Bulk load:'} | Set-Content $sqloutfile
    #write-host 'sqloutfile: ' $sqloutfile
    #write-host 'backuplistoutfile' $backuplistoutfile
    if ((test-path "$sqloutfile") -And (test-path $backuplistoutfile)) 
      {
        $filestoSFTP = $sqloutfile +' '+ $backuplistoutfile
        write-host 'Will start sftp now'
        xfer "$filestoSFTP" "/SQL Dba Team/ECA_MIG/" 
      }
    else
       {
        write-host "files for sftp MISSING"
        break
       }

      #now test for outfile existing and sftp it over  - full should already be there.
    #if(test-path "$sqloutfile" ) 
     # {
       
      #}
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
    
 
