
/*
##  TSQL SCRIPT: 1_CreateLoggingFramework.sql
## 
##  Description:  This creates the table and stored procedure for the audit framework
##                
##               
## 
##  Parameters : Varies
##
##  License:  
##
##  Repository: 
## 
##  Documentation on the  package 
## 
##  
##
##
##  Date				    Developer		  	Action
##  ---------------------------------------------------------------------
##  Jan 23, 2009			Steve Young			Final Version 1
##  Jun 04, 2013			Steve Young			Final Version 2 (SQL 2012)
##  Oct 07, 2017			Steve Young			Changes for SQL Azure & SQL 2016
##  Jun 21, 2020			Steve Young			Update
## 
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

 

*/

--Comment Out for SQL Azure
--USE [ADFTest_AzureImport] 
--GO


--  ===================================================================
--               Create Audit table
--  =================================================================== 



--Format for SQL 2016+ 
DROP TABLE IF EXISTS [dbo].[Audit_ProcessLog_Azure] 
go

CREATE TABLE [dbo].[Audit_ProcessLog_Azure] (
    [AuditID]                 BIGINT           IDENTITY (1, 1) NOT NULL,
    [TableName]               VARCHAR (100)     NOT NULL,
    [PkgName]                 VARCHAR (100)     NOT NULL,
    [CommandTxt]              VARCHAR (2000)   NULL,
	[RunStage]				  VARCHAR(100)      NULL,
    [ExecStartDT]             DATETIME         NOT NULL,
    [ExecStopDT]              DATETIME         NULL,
    [ExtractRowCnt]           BIGINT           NULL,
    [InsertRowCnt]            BIGINT           NULL,
    [UpdateRowCnt]            BIGINT           NULL,
    [ErrorRowCnt]             BIGINT           NULL,
    [TableInitialRowCnt]      BIGINT           NULL,
    [TableFinalRowCnt]        BIGINT           NULL,
    [TableMaxDateTime]        DATETIME         NULL,
    [SuccessfulProcessingInd] CHAR (1)         DEFAULT ('N') NOT NULL,
    [Notes]                   VARCHAR (1000)    NULL,
    [MasterBatchNumber]       BIGINT           NULL,
	[ChildBatchNumber]        BIGINT           NULL,
    [SQLErrorNumber]		  BIGINT           NULL,
    [SQLErrorLine]			  BIGINT           NULL,
    [SQLErrorMessage]	      VARCHAR(1024)    NULL, 
    [SQLErrorProcedure]       VARCHAR(1024)    NULL,
	[SQLErrorSeverity]	      BIGINT    NULL, 
    [SQLErrorState]           BIGINT   NULL,
    CONSTRAINT [PK_Audit_ProcessLog_Azure] PRIMARY KEY CLUSTERED ([AuditID] ASC)
);
go


ALTER TABLE [dbo].[Audit_ProcessLog_Azure] ADD  DEFAULT (('Unknown')) FOR [TableName]
GO

ALTER TABLE [dbo].[Audit_ProcessLog_Azure] ADD  DEFAULT (('Unknown')) FOR [PkgName]
GO

ALTER TABLE [dbo].[Audit_ProcessLog_Azure] ADD  DEFAULT (getdate()) FOR [ExecStartDT]
GO


--Format for SQL 2016+ 
DROP Procedure IF EXISTS [dbo].[usp_Insert_Audit_ProcessLog_Entry] 
go

CREATE PROCEDURE [dbo].[usp_Insert_Audit_ProcessLog_Entry]
(
	@_TableName as varchar(50),
	@_PkgName as varchar(50),
	@_CommandTxt varchar(2000),
    @_RunStage varchar(50),
	@_ExecStartDT as datetime,
	@_ExecStopDT as datetime,
	@_ExtractRowCnt as bigint,
	@_InsertRowCnt as bigint,
	@_UpdateRowCnt as bigint,
	@_ErrorRowCnt as bigint,
	@_TableInitialRowCnt as bigint,
	@_TableFinalRowCnt as bigint,
	@_TableMaxDateTime as datetime,
	@_SuccessfulProcessingInd as char(1),
	@_Notes as varchar(200),
	@_MasterBatchNumber as bigint,
	@_ChildBatchNumber as bigint,
    @_SQLErrorNumber  bigint,
	@_SQLErrorLine bigint,
    @_SQLErrorMessage varchar(1024), 
	@_SQLErrorProcedure varchar(1024),
	@_SQLErrorSeverity int,
	@_SQLErrorState int)


AS

INSERT INTO [dbo].[Audit_ProcessLog_Azure]
           ([TableName]
           ,[PkgName]
           ,[CommandTxt]
		   ,[RunStage] 
           ,[ExecStartDT]
           ,[ExecStopDT]
           ,[ExtractRowCnt]
           ,[InsertRowCnt]
           ,[UpdateRowCnt]
           ,[ErrorRowCnt]
           ,[TableInitialRowCnt]
           ,[TableFinalRowCnt]
           ,[TableMaxDateTime]
           ,[SuccessfulProcessingInd]
           ,[Notes]
           ,[MasterBatchNumber]
		   ,[ChildBatchNumber]
		   ,[SQLErrorNumber]
		   ,[SQLErrorLine]
		   ,[SQLErrorMessage]
		   ,[SQLErrorProcedure]
		   ,[SQLErrorSeverity] 
		   ,[SQLErrorState])
     VALUES
           (@_TableName,
			@_PkgName,
			@_CommandTxt,
			@_RunStage,
			@_ExecStartDT,
			@_ExecStopDT,
			@_ExtractRowCnt,
			@_InsertRowCnt,
			@_UpdateRowCnt,
			@_ErrorRowCnt,
			@_TableInitialRowCnt,
			@_TableFinalRowCnt,
			@_TableMaxDateTime,
			@_SuccessfulProcessingInd,
			@_Notes,
			@_MasterBatchNumber,
			@_ChildBatchNumber,
			@_SQLErrorNumber,
			@_SQLErrorLine,
			@_SQLErrorMessage, 
			@_SQLErrorProcedure,
			@_SQLErrorSeverity,
			@_SQLErrorState)
GO



--Format for SQL 2016+ 
DROP Table IF EXISTS[dbo].[Audit_GlobalVariables]
go

/****** Object:  Table [dbo].[Audit_GlobalVariables]    Script Date: 2017-10-30 8:31:03 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Audit_GlobalVariables](
	[VariableName] [varchar](50) NOT NULL,
	[VariableStringValue] [varchar](50) NULL,
	[VariableNumberValue] [int] NULL,
	[DatelastChanged] [datetime] NOT NULL,
	[DateAdded] [datetime] NOT NULL,
	[LastModifiedBy] [nvarchar](256) NULL,
	[VarID] [int] IDENTITY(1,1) NOT NULL,
	[IsSystem] [bit] NULL,
 CONSTRAINT [PK_GlobalVariables] PRIMARY KEY CLUSTERED 
(
	[VariableName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[Audit_GlobalVariables] ADD  DEFAULT (getdate()) FOR [DatelastChanged]
GO

ALTER TABLE [dbo].[Audit_GlobalVariables] ADD  DEFAULT (getdate()) FOR [DateAdded]
GO

ALTER TABLE [dbo].[Audit_GlobalVariables] ADD  DEFAULT ((0)) FOR [IsSystem]
GO



--Format for SQL 2016+ 
DROP PROCEDURE IF EXISTS  [dbo].[usp_Audit_BatchNumber] 
go

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





CREATE PROCEDURE [dbo].[usp_Audit_BatchNumber] 

(
@BatchNumber INT output
)
AS
BEGIN

	SET NOCOUNT ON;

Declare @ErrorCode int

 Select @BatchNumber = [VariableNumberValue]
  FROM [dbo].[Audit_GlobalVariables]
  Where [VariableName] = 'AuditBatchNumber'
  --Could not find variable
  --Error Handler
  
  IF( @@ROWCOUNT <> 1)
    Begin
		goto ERR_HANDLER
	End
  
  Update [dbo].[Audit_GlobalVariables]
  Set [VariableNumberValue] = [VariableNumberValue] + 1
  Where [VariableName] = 'AuditBatchNumber'

  --Could not Update variable
  --If @@error <> 0 goto ERR_HANDLER
   --Error Handler
  --Select @ErrorCode = @@Error
  --If (@ErrorCode <> 0)
  --  Begin
  --  goto ERR_HANDLER
  --End 
	IF( @@ROWCOUNT <> 1)
    Begin
		goto ERR_HANDLER
	End

  Return @BatchNumber



ERR_HANDLER:
   Select 'Unexpected error occurred!'
   Rollback transaction
   Set @BatchNumber = -1
   Return @BatchNumber

END
GO


--Insert the starter value

INSERT INTO [dbo].[Audit_GlobalVariables]
           ([VariableName]
           ,[VariableStringValue]
           ,[VariableNumberValue]
           ,[DatelastChanged]
           ,[DateAdded]
           ,[LastModifiedBy]
           ,[IsSystem])
     VALUES
           ('AuditBatchNumber'
           ,NULL
		   ,1
           ,getdate()
           ,getdate()
           ,'SYOUNG'
           ,0)
GO
