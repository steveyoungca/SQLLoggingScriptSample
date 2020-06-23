/*
##  TSQL SCRIPT: ImportCSVFromAzureBlog.sql
## 
##  Description:  
##      This script will take a single CSV file from SQL Blob storage and Bulk import
##		 the file to SQL 2017 or SQL Azure DB         
##               
## 
##  Parameters : Logging
##
##  License:  
##
##   Repository: 
## 
##  Documentation on the  package 
##  Documentation on the  package 
## https://blogs.msdn.microsoft.com/sqlserverstorageengine/2017/02/23/loading-files-from-azure-blob-storage-into-azure-sql-database/
## https://github.com/microsoft/sql-server-samples/tree/master/samples/features/sql-bulk-load/load-from-azure-blob-storage
## https://github.com/Microsoft/sql-server-samples/blob/master/samples/features/sql-bulk-load/load-from-azure-blob-storage/LoadFromAzureBlobStorage.sql
## https://docs.microsoft.com/en-us/sql/t-sql/statements/bulk-insert-transact-sql --with fmt file
## 
##
##  
##
##	Other Documentation 
##  --------------------
##  Shared Access Signature
##  https://docs.microsoft.com/en-us/azure/storage/common/storage-dotnet-shared-access-signature-part-1
## 
## 
## 
##  Date  				    Developer		  	Action
##  ---------------------------------------------------------------------
##  Oct 27, 2017			Steve Young			Initial Version
##  Jun 21, 2020			Steve Young			Changes for SQL Azure & SQL 2016
## 

##  TODO:
##	1. 
##	
##  Testing:
##  -------------------------------------------------------------
##    1. 
##		
##	Execute:  
##		1. 




USE [ADFTest_AzureImport]
GO

DECLARE @RC int
DECLARE @_Logging_FLAG char(1)

set @_Logging_FLAG = 'Y'

EXECUTE @RC = [dbo].[LOAD_Employee_From_Blob_storage] 
   @_Logging_FLAG
GO

Truncate table [dbo].[Employee] 
Select * from  [dbo].[Employee] 

 Select * from [dbo].[Audit_ProcessLog_Azure]







*/

--Comment Out for SQL Azure
USE [SEYLabTrainingDatabase]--[ADFTest_AzureImport] 
GO


--  ===================================================================
--               Create Stored Procedure
--  =================================================================== 


-- Drop stored procedure 
DROP PROCEDURE IF EXISTS [dbo].[LOAD_Employee_From_Blob_storage] 
GO 
  
-- Create stored procedure 
Create PROCEDURE [dbo].[LOAD_Employee_From_Blob_storage] 
  @_Logging_FLAG CHAR(1) = 'Y' 
AS 
BEGIN 
  

    --  ===================================================================   
	--               Declare Variables
	--  =================================================================== 
    DECLARE @RC int
	DECLARE @_TableName varchar(50)
	DECLARE @_PkgName varchar(50)
	DECLARE @_CommandTxt varchar(2000)
	DECLARE @_RunStage varchar(50)
	DECLARE @_ExecStartDT datetime
	DECLARE @_ExecStopDT datetime
	DECLARE @_ExtractRowCnt bigint
	DECLARE @_InsertRowCnt bigint
	DECLARE @_UpdateRowCnt bigint
	DECLARE @_ErrorRowCnt bigint
	DECLARE @_TableInitialRowCnt bigint
	DECLARE @_TableFinalRowCnt bigint
	DECLARE @_TableMaxDateTime datetime
	DECLARE @_SuccessfulProcessingInd char(1)
	DECLARE @_Notes varchar(200)
	DECLARE @_MasterBatchNumber bigint
	DECLARE @_ChildBatchNumber bigint
    DECLARE @_SQLErrorNumber  int
	DECLARE @_SQLErrorLine int
	DECLARE @_SQLErrorMessage varchar(1024) 
	DECLARE @_SQLErrorProcedure varchar(1024)
	DECLARE @_SQLErrorSeverity int 
	DECLARE @_SQLErrorState int 
	DECLARE @_Azure_Input_File VARCHAR(256); 
	DECLARE @_SQL_Statement NVARCHAR(1024); 

	--  ===================================================================   
	--               Setup 
	--  =================================================================== 

	--  Change for each package
	Set @_TableName = 'Employee'
	Set @_PkgName =  ' Azure Input Routine'
	Set @_RunStage = 'Package Start'
	--  Starting values
	Set @_CommandTxt = null
	Set @_ExecStartDT = getdate()
	Set @_ExecStopDT =  getdate()
	Set @_ExtractRowCnt = null
	Set @_InsertRowCnt = null
	Set @_UpdateRowCnt = null
	Set @_ErrorRowCnt = null
	Set @_TableInitialRowCnt = null
	Set @_TableFinalRowCnt = null
	Set @_TableMaxDateTime = null
	Set @_SuccessfulProcessingInd = 'Y'
	Set @_Notes = 'This is a test batch entry'
	EXEC [dbo].[usp_Audit_BatchNumber] @_MasterBatchNumber OUTPUT
	--Set @_MasterBatchNumber = 33
	Set @_ChildBatchNumber =2
	Set @_SQLErrorNumber = 222
	Set @_SQLErrorLine = 33
	Set @_SQLErrorMessage = null
	Set @_SQLErrorProcedure = null
	Set @_SQLErrorSeverity = 16
	Set @_SQLErrorState = 1
 

  -- No counting of rows 
  SET NOCOUNT ON; 
  
  -- Debugging 
  IF (@_Logging_FLAG = 'Y') 
  BEGIN 
    PRINT '[LOAD_FROM_BLOB_STORAGE] - STARTING TO EXECUTE STORED PROCEDURE.'; 
    PRINT ' '; 
  END; 
  
  -- ** ERROR HANDLING - START TRY ** 
  BEGIN TRY 
  
    -- Clear data 
    TRUNCATE TABLE [dbo].[Stage_Employee]; 
    SET @_Azure_Input_File = 'employeeDemo.csv'

 
      -- Debugging 
      IF (@_Logging_FLAG = 'Y') 
      BEGIN 
        PRINT '[LOAD_FROM_BLOB_STORAGE] - LOADING FILE ' + @_Azure_Input_File + '.'; 
        PRINT ' '; 
      END; 

     -- Create dynamic SQL statement 
      SELECT @_SQL_Statement = '  
      BULK INSERT [dbo].[Stage_Employee] 
      FROM ''' + @_Azure_Input_File + '''  
      WITH  
      (    
        DATA_SOURCE = ''AzureData_Employee'',  
        FORMAT = ''CSV'',  
        CODEPAGE = 65001,  
        FIRSTROW = 2,  
        TABLOCK  
      );'  
  
      -- Debugging 
      IF (@_Logging_FLAG = 'Y') 
      BEGIN 
        PRINT @_SQL_Statement 
        PRINT ' ' 
      END; 
  
      -- Execute Bulk Insert 
      EXEC SP_EXECUTESQL @_SQL_Statement; 
  
    



		--  ================= Write Successful Log Entry ================================  
	EXECUTE @RC = [dbo].[usp_Insert_Audit_ProcessLog_Entry] 
	   @_TableName,@_PkgName,@_CommandTxt,@_RunStage,@_ExecStartDT,@_ExecStopDT,@_ExtractRowCnt,@_InsertRowCnt,
	   @_UpdateRowCnt,@_ErrorRowCnt,@_TableInitialRowCnt,@_TableFinalRowCnt,@_TableMaxDateTime,@_SuccessfulProcessingInd,
	   @_Notes,@_MasterBatchNumber,@_ChildBatchNumber,@_SQLErrorNumber,@_SQLErrorLine,@_SQLErrorMessage,@_SQLErrorProcedure,
	   @_SQLErrorSeverity,@_SQLErrorState 

  
  
	--  ================= End Try ================================  
	END TRY 

    
	--  ================= Begin Catch ================================  
	BEGIN CATCH 
  
    --  ================= Grab values and Write Log Entry ================================
    SELECT 
      @_SQLErrorNumber = ERROR_NUMBER(), 
      @_SQLErrorProcedure = ERROR_PROCEDURE(), 
      @_SQLErrorLine = ERROR_LINE(), 
      @_SQLErrorMessage = ERROR_MESSAGE(), 
	  @_SQLErrorSeverity = 16,--ERROR_SEVERITY(),
	  @_SQLErrorState = 1,--ERROR_STATE(),
      @_SuccessfulProcessingInd = 'N'

	  --  ================= Send Error Log Entry ================================  
	  EXECUTE @RC = [dbo].[usp_Insert_Audit_ProcessLog_Entry] 
	   @_TableName,@_PkgName,@_CommandTxt,@_RunStage,@_ExecStartDT,@_ExecStopDT,@_ExtractRowCnt,@_InsertRowCnt,
	   @_UpdateRowCnt,@_ErrorRowCnt,@_TableInitialRowCnt,@_TableFinalRowCnt,@_TableMaxDateTime,@_SuccessfulProcessingInd,
	   @_Notes,@_MasterBatchNumber,@_ChildBatchNumber,@_SQLErrorNumber,@_SQLErrorLine,@_SQLErrorMessage,@_SQLErrorProcedure,
	   @_SQLErrorSeverity,@_SQLErrorState 

    -- Raise error 
    RAISERROR ('An error occurred within a user transaction. 
                Error Number        : %u 
                Error Message       : %s  
                Affected Procedure  : %s 
                Affected Line Number: %u
				Error Severity      : %u
				Error State         : %u' 
                , 16, 1 
                , @_SQLErrorNumber, @_SQLErrorMessage, @_SQLErrorProcedure, @_SQLErrorLine, @_SQLErrorSeverity, @_SQLErrorState);       
  
  -- ** Error Handling - End Catch **    
  END CATCH                          
END 
GO

