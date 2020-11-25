/*  Sourced from: https://wateroxconsulting.com/archives/whitelist/ 
	Create a trigger to check the IP address of the client against the DB whitelist */

CREATE TRIGGER IPAddress_Check ON ALL SERVER
    FOR LOGON
AS
    BEGIN
        DECLARE @ClientIP NVARCHAR(15);
        SET @ClientIP = ( SELECT    EVENTDATA().value('(/EVENT_INSTANCE/ClientHost)[1]',
                                                      'NVARCHAR(15)')
                        );
  /* first we check the actual IP address to see if it is whitelisted*/
        IF EXISTS ( SELECT  IPAddress
                    FROM    master.dbo.IPWhiteList
                    WHERE   IPAddress = @ClientIP )
            BEGIN
			/* IPAddress is in whitelist, logon allowed*/
                PRINT 'IP Address Allowed'
            END
        ELSE
            BEGIN
  /* now check for a range in our IPWhiteList Table and if the IP is in that range*/
                DECLARE @IPRange VARCHAR(15)
                SELECT  @IPRange = SUBSTRING(@ClientIP, 1,
                                             LEN(@ClientIP) - CHARINDEX('.',
                                                              REVERSE(@ClientIP)))
                        + '.*'
                IF EXISTS ( SELECT  IPAddress
                            FROM    master.dbo.IPWhiteList
                            WHERE   IPAddress = @IPRange )
                    BEGIN
                       /* IPAddress Range is in whitelist, logon allowed*/
                        PRINT 'IP Address Allowed'
                    END
                ELSE
  /* The IP is not in our whitelist therefore we go ahead log the IP. We can deny the connection by adding the commented out ROLLBACK
  statement back in, but then the logging will be rolled back as well.*/
                 MERGE MASTER.dbo.IP_Attempts IPA
                    USING
                        (SELECT @CLIENTIP) AS  CIP (IPAddress)
                    ON  IPA.IPAddress = CIP.IPAddress
                    WHEN NOT MATCHED THEN
						INSERT (IPAddress, Attempts)
						VALUES (CIP.IPAddress,1)
                    WHEN MATCHED THEN
						UPDATE SET Attempts = Attempts + 1;
				PRINT 'Your IP Address (' +ISNULL(@IPRange, @CLIENTIP)+ ') is blocked, Contact Administrator'
              --  ROLLBACK
            END
    END
GO 