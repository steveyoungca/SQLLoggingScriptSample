/*
##  TSQL SCRIPT: 3_Import_Data_DEV.sql
## 
##  Description:  Import Data to Stage Tables DEV
##                Creates Stored Procedure DEV_LOAD_Dev1_From_Blob_storage
##                This script will take a single CSV file from SQL Blob storage and Bulk import
##				  the file to SQL 2017 or SQL Azure DB 
## 
##  Parameters : Varies
##
##  License:  
##
##  Repository: 
## 
##  Prerequisite:  Need the Certificates and Share Access Signature 
## 
##  
##
##
##  Date				    Developer		  	Action
##  ---------------------------------------------------------------------
##  Nov 06, 2017			Steve Young			Initial Writing
##  Jun 21, 2020			Steve Young			Update
## 
## 
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
*/

--  ===================================================================
--               Settings & Declarations
--  =================================================================== 

/*    ==Scripting Parameters==

 
 
--  ===================================================================
--               Testing examples
--  =================================================================== 

 
DECLARE @RC int
DECLARE @_Logging_FLAG char(1)

set @_Logging_FLAG = 'N'

EXECUTE @RC = [dbo].[usp_DEV_LOAD_Dev1_From_Blob_storage] 
   @_Logging_FLAG
GO

-- Truncate table [dbo].[Dev_Stage_Dev1_1] 
-- Truncate table [dbo].[Audit_ProcessLog_Azure]

Select * from [dbo].[Dev_Stage_Dev1_1] 
Select * from [dbo].[Audit_ProcessLog_Azure]




*/


--  ===================================================================
--               Create Stored Procedure
--  =================================================================== 


-- Drop stored procedure 
DROP PROCEDURE IF EXISTS [dbo].[usp_DEV_LOAD_From_Blob_storage] 
GO 
  
-- Create stored procedure 
Create PROCEDURE [dbo].[usp_DEV_LOAD_From_Blob_storage] 
  @_Logging_FLAG CHAR(1) = 'N' 
AS 
BEGIN 
  

	--  ===================================================================   
	--               Declare Variables
	--  =================================================================== 
	DECLARE @RC int
	DECLARE @_TableName varchar(100)
	DECLARE @_PkgName varchar(100)
	DECLARE @_CommandTxt varchar(2000)
	DECLARE @_RunStage varchar(100)
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
	DECLARE @_Notes varchar(1000)
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
	DECLARE @_SQL_Statement_Truncate NVARCHAR(1024);
	--  ===================================================================   
	--               Setup 
	--  =================================================================== 

	--  !!!!!!!!!!!!!!!!! Change for each Stored Procedure !!!!!!!!!!!!!!!!!!!!!!!
	--  Change for each package
	Set @_TableName = '[dbo].[Dev_Stage_Dev1_1]' --'[dbo].[Employee]'
	Set @_PkgName =  'usp_DEV_LOAD_Dev1_From_Blob_storage'
	Set @_RunStage = 'Package Start'
	SET @_Azure_Input_File = 'Dev1_1.csv' --'employeeDemo.csv'
	
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
	Set @_SuccessfulProcessingInd = 'N'
	Set @_Notes = 'This is a test batch entry with table ' + @_Azure_Input_File
	EXEC [dbo].[usp_Audit_BatchNumber] @_MasterBatchNumber OUTPUT
	--Set @_MasterBatchNumber = 33
	Set @_ChildBatchNumber = null
	Set @_SQLErrorNumber = null
	Set @_SQLErrorLine = null
	Set @_SQLErrorMessage = null
	Set @_SQLErrorProcedure = null
	Set @_SQLErrorSeverity = null
	Set @_SQLErrorState = null
 

  -- No counting of rows 
  SET NOCOUNT ON; 
  
  -- Debugging 
  IF (@_Logging_FLAG = 'Y') 
  BEGIN 
	PRINT '[LOAD_FROM_BLOB_STORAGE] - STARTING TO EXECUTE STORED PROCEDURE.'; 
	PRINT ' '; 
  END; 
	  
	  --  !!!!!!!!!!!!!!!!! Change for each Stored Procedure !!!!!!!!!!!!!!!!!!!!!!!
	  --  ================= Starting Rows Table ================================ 
	 Select  @_TableInitialRowCnt =  Count(*) From [dbo].[Dev_Stage_Dev1_1] 

  --  ================= Error Handle Begin Try ================================  
  BEGIN TRY  


	 --  !!!!!!!!!!!!!!!!! Change for each Stored Procedure !!!!!!!!!!!!!!!!!!!!!!!
	 --  ================= Truncate Table ================================ 
	 -- Delete so that the records hit the log
	  Delete  [dbo].[Dev_Stage_Dev1_1] 

	  --SET @_Azure_Input_File = 'employeeDemo.csv'
	  -- Debugging 
	  IF (@_Logging_FLAG = 'Y') 
	  BEGIN 
		PRINT '[LOAD_FROM_BLOB_STORAGE] - LOADING FILE ' + @_Azure_Input_File + '.'; 
		PRINT ' '; 
	  END; 

	 -- Create dynamic SQL statement 
	  SELECT @_SQL_Statement = '  
	  BULK INSERT ' + @_TableName + ' 
	  FROM ''' + @_Azure_Input_File + '''  
	  WITH  
	  (    
		DATA_SOURCE = ''AzureData_Source'',  
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
	
		--  ================= Grab Row Changed ================================ 
		Select @_InsertRowCnt = @@ROWCOUNT
		
		--  !!!!!!!!!!!!!!!!! Change for each Stored Procedure !!!!!!!!!!!!!!!!!!!!!!!
		--  ================= Starting Rows Table ================================ 
	 Select  @_TableFinalRowCnt =  Count(*) From [dbo].[Dev_Stage_Dev1_1] 
	
	  --  ================= Setup Successful Log Entry ================================ 
	  Set @_ExecStopDT =  getdate()
	  Set @_SuccessfulProcessingInd = 'Y'

		--  ================= Write Successful Log Entry ================================  
	EXECUTE @RC = [dbo].[usp_Insert_Audit_ProcessLog_Entry] 
	   @_TableName,@_PkgName,@_SQL_Statement,@_RunStage,@_ExecStartDT,@_ExecStopDT,@_ExtractRowCnt,@_InsertRowCnt,
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

