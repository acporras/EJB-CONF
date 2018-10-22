USE comercial
GO

ALTER DATABASE comercial
SET COMPATIBILITY_LEVEL = 100 -- For SQL Server 2008 R2
GO

IF EXISTS (SELECT * FROM sysobjects WHERE name='STRING_SPLIT') BEGIN
	DROP FUNCTION STRING_SPLIT
END
GO
CREATE FUNCTION STRING_SPLIT ( @stringToSplit VARCHAR(MAX), @delimiter CHAR(1) )
RETURNS
@returnList TABLE ([value] [nvarchar] (1500))
AS
BEGIN

 DECLARE @name NVARCHAR(255)
 DECLARE @pos INT

 WHILE CHARINDEX(@delimiter, @stringToSplit) > 0
 BEGIN
  SELECT @pos  = CHARINDEX(@delimiter, @stringToSplit)  
  SELECT @name = SUBSTRING(@stringToSplit, 1, @pos-1)

  INSERT INTO @returnList 
  SELECT @name

  SELECT @stringToSplit = SUBSTRING(@stringToSplit, @pos+1, LEN(@stringToSplit)-@pos)
 END

 INSERT INTO @returnList
 SELECT @stringToSplit

 RETURN
END
GO

IF EXISTS (SELECT * FROM sysobjects WHERE name='SPS_TABFACCAB_BY_ESTDOCELE') BEGIN
	DROP PROCEDURE SPS_TABFACCAB_BY_ESTDOCELE
END
GO
CREATE PROCEDURE SPS_TABFACCAB_BY_ESTDOCELE(
	@TX_ESTDOCELE VARCHAR(150),
	@NO_DOCELECAB VARCHAR(50)
)
AS
BEGIN
	DECLARE @TBL_DOCELECAB NVARCHAR(MAX);
	SET @TBL_DOCELECAB = 'SELECT * FROM ' + @NO_DOCELECAB + ' WHERE FA1_ESTADO_ENVIO IS NULL OR FA1_ESTADO_ENVIO NOT IN(
		SELECT value  
		FROM STRING_SPLIT(''' + @TX_ESTDOCELE + ''', '','')  
		WHERE RTRIM(value) <> ''''
	)'
	EXEC SP_EXECUTESQL @TBL_DOCELECAB
END
GO

IF EXISTS (SELECT * FROM sysobjects WHERE name='SPS_TABFACDET_BY_TABFACCAB') BEGIN
	DROP PROCEDURE SPS_TABFACDET_BY_TABFACCAB
END
GO
CREATE PROCEDURE SPS_TABFACDET_BY_TABFACCAB(
	@CO_DETALTIDO CHAR(2),
	@NU_DETSERSUN CHAR(4),
	@NU_DETNUMSUN CHAR(7),
	@NO_DOCELEDET VARCHAR(50)
)
AS
BEGIN
	DECLARE @TBL_DOCELEDET NVARCHAR(MAX);
	SET @TBL_DOCELEDET = 'SELECT * FROM ' + @NO_DOCELEDET +
	' WHERE FA2_CTIPDOC = ''' + @CO_DETALTIDO + '''' +
	' AND FA2_CSERDOC = ''' + @NU_DETSERSUN + '''' +
	' AND FA2_CNUMDOC = ''' + @NU_DETNUMSUN + ''''
	EXEC SP_EXECUTESQL @TBL_DOCELEDET
END
GO

IF EXISTS (SELECT * FROM sysobjects WHERE name='SPU_TABFACCAB_MIG') BEGIN
	DROP PROCEDURE SPU_TABFACCAB_MIG
END
GO
CREATE PROCEDURE SPU_TABFACCAB_MIG(
	@CO_DOCALTIDO CHAR(2),
	@NU_DOCSERSUN CHAR(4),
	@NU_DOCNUMSUN CHAR(7),
	@NO_DOCELECAB VARCHAR(50)
)
AS
BEGIN
	DECLARE @UPD_DOCELECAB NVARCHAR(MAX);
	SET @UPD_DOCELECAB = 'UPDATE ' +  @NO_DOCELECAB +
	' SET FA1_ESTADO_ENVIO = 4' +
	' WHERE FA1_CTIPDOC = ''' + @CO_DOCALTIDO + '''' +
	' AND FA1_CSERDOC = ''' + @NU_DOCSERSUN + '''' +
	' AND FA1_CNUMDOC = ''' + @NU_DOCNUMSUN + ''''
	EXEC SP_EXECUTESQL @UPD_DOCELECAB
END
GO