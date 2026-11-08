-- Fixed objects: never interpolate channel names into SQL identifiers.
IF NOT EXISTS (SELECT 1 FROM sys.service_message_types WHERE name = 'DispatcherdPayload')
    CREATE MESSAGE TYPE [DispatcherdPayload] VALIDATION = NONE;
IF NOT EXISTS (SELECT 1 FROM sys.service_contracts WHERE name = 'DispatcherdContract')
    CREATE CONTRACT [DispatcherdContract] ([DispatcherdPayload] SENT BY INITIATOR);
IF OBJECT_ID('dbo.DispatcherdInbox') IS NULL
    CREATE QUEUE dbo.DispatcherdInbox WITH RETENTION = OFF;
IF OBJECT_ID('dbo.DispatcherdOutbox') IS NULL
    CREATE QUEUE dbo.DispatcherdOutbox WITH RETENTION = OFF;
IF NOT EXISTS (SELECT 1 FROM sys.services WHERE name = 'DispatcherdListener')
    CREATE SERVICE [DispatcherdListener] ON QUEUE dbo.DispatcherdInbox ([DispatcherdContract]);
IF NOT EXISTS (SELECT 1 FROM sys.services WHERE name = 'DispatcherdPublisher')
    CREATE SERVICE [DispatcherdPublisher] ON QUEUE dbo.DispatcherdOutbox;
IF OBJECT_ID('dbo.DispatcherdListenerRegistry') IS NULL
    CREATE TABLE dbo.DispatcherdListenerRegistry (
        conversation_id uniqueidentifier NOT NULL PRIMARY KEY,
        node_id nvarchar(256) NULL,
        created_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
IF OBJECT_ID('dbo.DispatcherdSubscription') IS NULL
BEGIN
    CREATE TABLE dbo.DispatcherdSubscription (
        conversation_id uniqueidentifier NOT NULL REFERENCES dbo.DispatcherdListenerRegistry(conversation_id) ON DELETE CASCADE,
        channel nvarchar(256) COLLATE Latin1_General_100_BIN2 NOT NULL,
        PRIMARY KEY(channel, conversation_id)
    );
END;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('dbo.DispatcherdSubscription') AND name='IX_DispatcherdSubscription_Instance')
    CREATE INDEX IX_DispatcherdSubscription_Instance ON dbo.DispatcherdSubscription(conversation_id);
IF DATABASE_PRINCIPAL_ID('dispatcherd_runtime') IS NULL CREATE ROLE dispatcherd_runtime;

IF NOT EXISTS (SELECT 1 FROM sys.services WHERE name='DispatcherdListener' AND service_queue_id=OBJECT_ID('dbo.DispatcherdInbox'))
    OR NOT EXISTS (SELECT 1 FROM sys.services WHERE name='DispatcherdPublisher' AND service_queue_id=OBJECT_ID('dbo.DispatcherdOutbox'))
    THROW 50002, 'Existing Dispatcherd services use incompatible queues', 1;
IF NOT EXISTS (SELECT 1 FROM sys.service_message_types WHERE name='DispatcherdPayload' AND validation='N')
    OR NOT EXISTS (SELECT 1 FROM sys.service_contract_message_usages u
        JOIN sys.service_contracts c ON c.service_contract_id=u.service_contract_id
        JOIN sys.service_message_types m ON m.message_type_id=u.message_type_id
        WHERE c.name='DispatcherdContract' AND m.name='DispatcherdPayload' AND u.is_sent_by_initiator=1)
    THROW 50003, 'Existing Dispatcherd message type or contract is incompatible', 1;

GO
CREATE OR ALTER PROCEDURE dbo.DispatcherdRegister
    @node nvarchar(256), @channels nvarchar(max), @check varbinary(max)
WITH EXECUTE AS OWNER AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRANSACTION;
    DECLARE @h uniqueidentifier, @id uniqueidentifier;
    BEGIN DIALOG CONVERSATION @h
        FROM SERVICE [DispatcherdPublisher] TO SERVICE 'DispatcherdListener'
        ON CONTRACT [DispatcherdContract] WITH ENCRYPTION = OFF;
    SELECT @id=conversation_id FROM sys.conversation_endpoints WHERE conversation_handle=@h;
    INSERT dbo.DispatcherdListenerRegistry(conversation_id,node_id) VALUES (@id,@node);
    -- Force point lookups for the foreign-key check to avoid deadlocks between
    -- simultaneous registrations holding their own metadata rows.
    INSERT dbo.DispatcherdSubscription SELECT @id,value FROM OPENJSON(@channels) OPTION (LOOP JOIN);
    -- Initial reactive self-check. No persistent readiness state or publication gate.
    SEND ON CONVERSATION @h MESSAGE TYPE [DispatcherdPayload] (@check);
    COMMIT;
    SELECT @id AS instance_id;
END;
GO
CREATE OR ALTER PROCEDURE dbo.DispatcherdSnapshot @channel nvarchar(256)=NULL
WITH EXECUTE AS OWNER AS
BEGIN
    SET NOCOUNT ON;
    SELECT r.conversation_id AS instance_id,r.node_id,p.conversation_handle AS publisher_handle,
        t.conversation_handle AS receiver_handle,r.created_at,
        (SELECT channel FROM dbo.DispatcherdSubscription s WHERE s.conversation_id=r.conversation_id FOR JSON PATH) AS channels
    FROM dbo.DispatcherdListenerRegistry r
    LEFT JOIN sys.conversation_endpoints p ON p.conversation_id=r.conversation_id AND p.is_initiator=1
    LEFT JOIN sys.conversation_endpoints t ON t.conversation_id=r.conversation_id AND t.is_initiator=0
    WHERE @channel IS NULL OR EXISTS
        (SELECT 1 FROM dbo.DispatcherdSubscription s WHERE s.conversation_id=r.conversation_id AND channel=@channel);
END;
GO
CREATE OR ALTER PROCEDURE dbo.DispatcherdSend
    @id uniqueidentifier, @channel nvarchar(256), @body varbinary(max)
WITH EXECUTE AS OWNER AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    -- Hold the registration through SEND, serializing publication against retirement.
    -- This also participates in a caller-owned transaction without committing it.
    BEGIN TRANSACTION;
    DECLARE @h uniqueidentifier;
    SELECT @h=p.conversation_handle FROM dbo.DispatcherdListenerRegistry r WITH (UPDLOCK,HOLDLOCK)
        JOIN sys.conversation_endpoints p ON p.conversation_id=r.conversation_id AND p.is_initiator=1
        WHERE r.conversation_id=@id
        AND EXISTS (SELECT 1 FROM dbo.DispatcherdSubscription s WHERE s.conversation_id=@id AND channel=@channel);
    IF @h IS NOT NULL AND EXISTS
        (SELECT 1 FROM sys.conversation_endpoints WHERE conversation_handle=@h AND state IN ('CO','SO'))
    BEGIN
        SEND ON CONVERSATION @h MESSAGE TYPE [DispatcherdPayload] (@body);
        SELECT CAST(1 AS int) AS sent;
    END
    ELSE SELECT CAST(0 AS int) AS sent;
    COMMIT;
END;
GO
CREATE OR ALTER PROCEDURE dbo.DispatcherdReceive @id uniqueidentifier, @timeout int
WITH EXECUTE AS OWNER AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
    DECLARE @h uniqueidentifier;
    IF NOT EXISTS (SELECT 1 FROM dbo.DispatcherdListenerRegistry WHERE conversation_id=@id)
        THROW 50001, 'Listener registration is unavailable', 1;
    SELECT @h=conversation_handle FROM sys.conversation_endpoints WHERE conversation_id=@id AND is_initiator=0;
    IF @h IS NULL
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM sys.conversation_endpoints WHERE conversation_id=@id AND is_initiator=1 AND state IN ('SO','CO'))
            THROW 50001, 'Listener conversation is unavailable', 1;
        -- The first SEND may still be materializing the target. Bound polling.
        DECLARE @delay time(3)=DATEADD(millisecond, CASE WHEN @timeout<20 THEN @timeout ELSE 20 END, CAST('00:00:00' AS time(3)));
        WAITFOR DELAY @delay;
        RETURN;
    END;
    WAITFOR (RECEIVE TOP(1) conversation_handle,message_type_name,message_body
        FROM dbo.DispatcherdInbox WHERE conversation_handle=@h), TIMEOUT @timeout;
    END TRY
    BEGIN CATCH
        IF ERROR_NUMBER() <> 1222 THROW;
        -- A concurrent uncommitted SEND can lock the queue beyond WAITFOR's
        -- timeout. Treat only this bounded lock timeout as an empty receive.
        SELECT CAST(NULL AS uniqueidentifier) AS conversation_handle,
            CAST(NULL AS nvarchar(256)) AS message_type_name,
            CAST(NULL AS varbinary(max)) AS message_body WHERE 1=0;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE dbo.DispatcherdRetire @id uniqueidentifier
WITH EXECUTE AS OWNER AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRANSACTION;
    DECLARE @p uniqueidentifier, @r uniqueidentifier, @c uniqueidentifier;
    SELECT @c=conversation_id FROM dbo.DispatcherdListenerRegistry WITH (UPDLOCK,HOLDLOCK)
        WHERE conversation_id=@id;
    SELECT @p=conversation_handle FROM sys.conversation_endpoints WHERE conversation_id=@c AND is_initiator=1;
    -- Also handles a process lost before receiving its initial self-check.
    SELECT @r=conversation_handle FROM sys.conversation_endpoints WHERE conversation_id=@c AND is_initiator=0;
    IF EXISTS (SELECT 1 FROM sys.conversation_endpoints WHERE conversation_handle=@r AND state <> 'CD')
        END CONVERSATION @r;
    IF EXISTS (SELECT 1 FROM sys.conversation_endpoints WHERE conversation_handle=@p AND state <> 'CD')
        END CONVERSATION @p;
    DELETE dbo.DispatcherdListenerRegistry WHERE conversation_id=@id;
    COMMIT;
END;
GO
GRANT EXECUTE ON dbo.DispatcherdRegister TO dispatcherd_runtime;
GRANT EXECUTE ON dbo.DispatcherdSnapshot TO dispatcherd_runtime;
GRANT EXECUTE ON dbo.DispatcherdSend TO dispatcherd_runtime;
GRANT EXECUTE ON dbo.DispatcherdReceive TO dispatcherd_runtime;
GRANT EXECUTE ON dbo.DispatcherdRetire TO dispatcherd_runtime;
GO
-- Reply callers are ephemeral endpoints, not registered service instances.
IF NOT EXISTS (SELECT 1 FROM sys.service_contracts WHERE name='DispatcherdReplyContract')
    CREATE CONTRACT [DispatcherdReplyContract] ([DispatcherdPayload] SENT BY INITIATOR);
IF NOT EXISTS (SELECT 1 FROM sys.services WHERE name='DispatcherdReplyListener')
    CREATE SERVICE [DispatcherdReplyListener] ON QUEUE dbo.DispatcherdInbox ([DispatcherdReplyContract]);
IF NOT EXISTS (SELECT 1 FROM sys.services WHERE name='DispatcherdReplyListener' AND service_queue_id=OBJECT_ID('dbo.DispatcherdInbox'))
    OR NOT EXISTS (SELECT 1 FROM sys.service_contract_message_usages u
        JOIN sys.service_contracts c ON c.service_contract_id=u.service_contract_id
        JOIN sys.service_message_types m ON m.message_type_id=u.message_type_id
        WHERE c.name='DispatcherdReplyContract' AND m.name='DispatcherdPayload' AND u.is_sent_by_initiator=1)
    THROW 50007, 'Existing reply service or contract is incompatible', 1;
GO
CREATE OR ALTER PROCEDURE dbo.DispatcherdOpenReply @lifetime int=300
WITH EXECUTE AS OWNER AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF @lifetime < 1 OR @lifetime > 300 THROW 50004, 'Reply lifetime must be 1 to 300 seconds', 1;
    BEGIN TRANSACTION;
    DECLARE @h uniqueidentifier;
    BEGIN DIALOG CONVERSATION @h FROM SERVICE [DispatcherdPublisher]
        TO SERVICE 'DispatcherdReplyListener' ON CONTRACT [DispatcherdReplyContract]
        WITH ENCRYPTION=OFF, LIFETIME=@lifetime;
    SEND ON CONVERSATION @h MESSAGE TYPE [DispatcherdPayload] (0x7B7D);
    SELECT @h AS publisher_handle,conversation_id FROM sys.conversation_endpoints WHERE conversation_handle=@h;
    COMMIT;
END;
GO
CREATE OR ALTER PROCEDURE dbo.DispatcherdReplyReady @publisher uniqueidentifier
WITH EXECUTE AS OWNER AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @receiver uniqueidentifier;
    SELECT @receiver=t.conversation_handle FROM sys.conversation_endpoints p
        JOIN sys.conversation_endpoints t ON t.conversation_id=p.conversation_id AND t.is_initiator=0
        WHERE p.conversation_handle=@publisher AND p.is_initiator=1
        AND p.service_contract_id=(SELECT service_contract_id FROM sys.service_contracts WHERE name='DispatcherdReplyContract');
    -- Caller invokes once per handshake; the hello is consumed before publishing the address.
    IF @receiver IS NOT NULL
    BEGIN
        DECLARE @hello varbinary(max);
        RECEIVE TOP(1) @hello=message_body FROM dbo.DispatcherdInbox WHERE conversation_handle=@receiver;
        IF @hello IS NOT NULL SELECT @receiver AS receiver_handle;
    END;
END;
GO
CREATE OR ALTER PROCEDURE dbo.DispatcherdSendReply @publisher uniqueidentifier, @body varbinary(max)
WITH EXECUTE AS OWNER AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @outer_transaction int=@@TRANCOUNT;
    BEGIN TRANSACTION;
    DECLARE @lock int, @resource nvarchar(255)=N'dispatcherd_reply_'+CONVERT(nvarchar(36),@publisher);
    EXEC @lock=sys.sp_getapplock @Resource=@resource,@LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=5000;
    IF @lock < 0 THROW 50005, 'Could not lock reply conversation', 1;
    IF EXISTS (SELECT 1 FROM sys.conversation_endpoints WHERE conversation_handle=@publisher
        AND is_initiator=1 AND state IN ('CO','SO') AND lifetime>GETUTCDATE()
        AND service_id=(SELECT service_id FROM sys.services WHERE name='DispatcherdPublisher')
        AND service_contract_id=(SELECT service_contract_id FROM sys.service_contracts WHERE name='DispatcherdReplyContract'))
    BEGIN
        BEGIN TRY
            SEND ON CONVERSATION @publisher MESSAGE TYPE [DispatcherdPayload] (@body);
            SELECT 1 AS sent;
        END TRY
        BEGIN CATCH
            -- Lifetime expiration is asynchronous; it can race the catalog check.
            IF ERROR_NUMBER() <> 8429 OR @outer_transaction > 0 THROW;
            -- Only an owned autocommit send may absorb the late-expiry race.
            -- Never silently roll back a caller-owned transaction.
            IF XACT_STATE() <> 0 ROLLBACK;
            SELECT 0 AS sent;
            RETURN;
        END CATCH;
    END
    ELSE SELECT 0 AS sent;
    COMMIT;
END;
GO
CREATE OR ALTER PROCEDURE dbo.DispatcherdReceiveReply @receiver uniqueidentifier, @timeout int
WITH EXECUTE AS OWNER AS
BEGIN
    SET NOCOUNT ON;
    IF NOT EXISTS (SELECT 1 FROM sys.conversation_endpoints WHERE conversation_handle=@receiver AND is_initiator=0
        AND service_contract_id=(SELECT service_contract_id FROM sys.service_contracts WHERE name='DispatcherdReplyContract'))
        THROW 50006, 'Reply endpoint is unavailable', 1;
    BEGIN TRY
        WAITFOR (RECEIVE TOP(1) conversation_handle,message_type_name,message_body
            FROM dbo.DispatcherdInbox WHERE conversation_handle=@receiver), TIMEOUT @timeout;
    END TRY
    BEGIN CATCH
        IF ERROR_NUMBER() <> 1222 THROW;
        SELECT CAST(NULL AS uniqueidentifier) AS conversation_handle, CAST(NULL AS nvarchar(256)) AS message_type_name,
            CAST(NULL AS varbinary(max)) AS message_body WHERE 1=0;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE dbo.DispatcherdCloseReply @publisher uniqueidentifier
WITH EXECUTE AS OWNER AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRANSACTION;
    DECLARE @lock int, @resource nvarchar(255)=N'dispatcherd_reply_'+CONVERT(nvarchar(36),@publisher);
    EXEC @lock=sys.sp_getapplock @Resource=@resource,@LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=5000;
    IF @lock < 0 THROW 50005, 'Could not lock reply conversation', 1;
    DECLARE @c uniqueidentifier, @receiver uniqueidentifier;
    SELECT @c=conversation_id FROM sys.conversation_endpoints WHERE conversation_handle=@publisher AND is_initiator=1
        AND service_contract_id=(SELECT service_contract_id FROM sys.service_contracts WHERE name='DispatcherdReplyContract');
    SELECT @receiver=conversation_handle FROM sys.conversation_endpoints WHERE conversation_id=@c AND is_initiator=0;
    IF EXISTS (SELECT 1 FROM sys.conversation_endpoints WHERE conversation_handle=@receiver AND state<>'CD')
        END CONVERSATION @receiver;
    IF EXISTS (SELECT 1 FROM sys.conversation_endpoints WHERE conversation_handle=@publisher AND conversation_id=@c AND state<>'CD')
        END CONVERSATION @publisher;
    COMMIT;
END;
GO
CREATE OR ALTER PROCEDURE dbo.DispatcherdExpiredReplies
WITH EXECUTE AS OWNER AS
BEGIN
    SET NOCOUNT ON;
    -- Include a closed initiator if its partner still needs termination.
    SELECT p.conversation_handle AS publisher_handle FROM sys.conversation_endpoints p
    WHERE p.is_initiator=1 AND p.lifetime<=GETUTCDATE()
        AND p.service_contract_id=(SELECT service_contract_id FROM sys.service_contracts WHERE name='DispatcherdReplyContract')
        AND (p.state<>'CD' OR EXISTS (SELECT 1 FROM sys.conversation_endpoints t WHERE t.conversation_id=p.conversation_id AND t.state<>'CD'));
END;
GO
GRANT EXECUTE ON dbo.DispatcherdOpenReply TO dispatcherd_runtime;
GRANT EXECUTE ON dbo.DispatcherdReplyReady TO dispatcherd_runtime;
GRANT EXECUTE ON dbo.DispatcherdSendReply TO dispatcherd_runtime;
GRANT EXECUTE ON dbo.DispatcherdReceiveReply TO dispatcherd_runtime;
GRANT EXECUTE ON dbo.DispatcherdCloseReply TO dispatcherd_runtime;
GRANT EXECUTE ON dbo.DispatcherdExpiredReplies TO dispatcherd_runtime;
