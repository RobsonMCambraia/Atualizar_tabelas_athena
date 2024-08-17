create table prevencao_a_fraude.filial as (
WITH FILIAIS AS (
	SELECT DISTINCT
	    FIL.FILI_CD_FILIAL AS FILIAL,
	    COUNT(US.USUA_TX_MATRICULA) over (partition by FIL.FILI_CD_FILIAL) as QTD_COLAB,
	    FIL.ESTA_SG_UF AS UF,
	    FIL.FILI_NM_FANTASIA AS NOME_FILIAL,
	    CONCAT(FIL.FILI_TP_LOGRAD, ' ', 
	           FIL.FILI_NM_LOGRAD, ', ', 
	           FIL.FILI_TX_NR_LOGRAD, ' - ', 
	           FIL.FILI_NM_BAIRRO, ', ', 'CEP ', 
	           FIL.FILI_TN_CEP) AS ENDERECO,
	    FIL.FILI_TN_CNPJ AS CNPJ,
	    MU.MUNI_NM_MUNICIPIO,
	    ES.REGIAO,
	    FIL.FILI_TN_CEP,
	    FIL.ESTA_SG_UF,
	    UPPER(SUBSTR(REPLACE(ES.ESTA_NM_ESTADO, 'ã', 'a'), 1, 1)) || 
	    LOWER(SUBSTR(REPLACE(ES.ESTA_NM_ESTADO, 'ã', 'a'), 2)) AS ESTADO,
	    CASE 
	        WHEN FIL.EMPR_CD_BANDEIRA = 1 THEN 'PGMN'
	        WHEN FIL.EMPR_CD_BANDEIRA = 2 THEN 'EF'
	    END AS BANDEIRA,
	    FIL.FILI_FL_SITUACAO AS SITUACAO,
	    DH.MatriculaGL,
	    DH.NomeGL AS GL,
	    DH.MatriculaGR,
	    DH.NomeGR AS GR,
	    DH.MatriculaGO,
	    DH.NomeGO AS GO,
	    DH.Area
	FROM modelled.cosmos_v14b_dbo_filial FIL
	INNER JOIN modelled.cosmos_v14b_dbo_estado ES ON ES.esta_sg_uf = FIL.esta_sg_uf
	INNER JOIN modelled.cosmos_v14b_dbo_municipio MU ON MU.id_estado = ES.id_estado AND MU.id_municipio = FIL.id_municipio
	INNER JOIN modelled.pm_dw_dbo_dim_hierarquia DH ON DH.codigofilial = FIL.fili_cd_filial
	inner join modelled.cosmos_v14b_dbo_usuario US on US.FILIAL_ORIGEM = FIL.FILI_CD_FILIAL
	WHERE FIL.fili_fl_situacao = 'A' and US.USUA_DT_RESCIS IS NULL)
SELECT * FROM FILIAIS F);