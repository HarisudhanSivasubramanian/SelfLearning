Set NOCOUNT ON

 DECLARE @Filename NVARCHAR(50)
 DECLARE @fileFullPath NVARCHAR(100)

 SELECT @Filename = N'npidata_pfile_20050523-20180611.csv'
 SELECT @fileFullPath = N'Z:\SQLTempFiles\npidata_pfile_20050523-20180611.csv'

create table #dir

(output varchar(2000))

 DECLARE @cmd NVARCHAR(100)
SELECT @cmd = 'dir ' + @fileFullPath     

insert into #dir    

exec master.dbo.xp_cmdshell @cmd

--Select * from #dir

-- This is risky, as the fle path itself might contain the filename
if exists (Select * from #dir where output like '%'+ @Filename +'%')

       begin    
              Print 'File found'    
              --Add code you want to run if file exists    
       end    
else    
       begin    
              Print 'No File Found'    
              --Add code you want to run if file does not exists    
       end

drop table #dir