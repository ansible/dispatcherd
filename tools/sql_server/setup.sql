IF DB_ID(N'dispatcherd') IS NULL
BEGIN
    CREATE DATABASE [dispatcherd];
END;
GO

USE [dispatcherd];
GO

IF OBJECT_ID(N'dbo.dispatcherd_messages', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[dispatcherd_messages] (
        [id] BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT [PK_dispatcherd_messages] PRIMARY KEY,
        [channel] NVARCHAR(200) NOT NULL,
        [payload] NVARCHAR(MAX) NOT NULL,
        [created_at] DATETIME2 NOT NULL
            CONSTRAINT [DF_dispatcherd_messages_created_at] DEFAULT (SYSUTCDATETIME())
    );
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_dispatcherd_messages_channel_id'
      AND [object_id] = OBJECT_ID(N'dbo.dispatcherd_messages')
)
BEGIN
    CREATE INDEX [IX_dispatcherd_messages_channel_id]
        ON [dbo].[dispatcherd_messages] ([channel], [id]);
END;
GO
