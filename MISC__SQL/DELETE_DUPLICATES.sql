WITH cte AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY [dataMessageGUID], [sensorID], [dataTypeID], [plotLabelID], [messageDate], [rawData], [dataValue], [plotValue]
			ORDER BY [dataMessageGUID], [sensorID], [dataTypeID], [plotLabelID], [messageDate], [rawData], [dataValue], [plotValue]) rownum
    FROM [salfordMove].[dbo].[READINGS]
)
/*SELECT
  * 
FROM
    cte 
WHERE
    rownum > 1;
*/
DELETE FROM cte
WHERE rownum > 1;
