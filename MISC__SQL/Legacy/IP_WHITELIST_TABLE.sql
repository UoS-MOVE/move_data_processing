/*  Sourced from: https://wateroxconsulting.com/archives/whitelist/ 
	Create whitelist table for IPs permitted to conect to the DB */

CREATE TABLE master.dbo.IP_WHITELIST (ipAddress NVARCHAR(15), ipID UNIQUEIDENTIFIER, ipName NVARCHAR(15), ipDescription NVARCHAR(50));
GO

GRANT SELECT ON master.dbo.IP_WHITELIST TO PUBLIC;
GO


/* Create table to track attempts to connect from non-whitelisted IPs */
CREATE TABLE master.dbo.IP_Attempts ( IPAddress VARCHAR(15), Attempts int );
GO

GRANT INSERT ON MASTER.dbo.IP_Attempts TO PUBLIC;
GRANT SELECT ON MASTER.dbo.IP_Attempts TO PUBLIC;
GRANT UPDATE ON MASTER.dbo.IP_Attempts TO PUBLIC;	
GO

