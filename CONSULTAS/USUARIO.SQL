create table prevencao_a_fraude.usuario as
SELECT
    US.USUA_TX_MATRICULA AS MAT,
    US.USUA_NM_USUARIO AS NOME,
    US.CARG_CD_CARGO AS CD_CARGO,
    CG.CARG_NM_CARGO AS NM_CARGO,
    US.LTUS_CD_LOT_USU AS LOTACAO,
    LT.LTUS_NM_LOTACAO AS NM_LOTACAO,
    US.FILIAL_ORIGEM AS FIL_ORI,
    US.USUA_CD_FILIAL,
    date_format(US.USUA_DT_ADMISS, '%d/%m/%Y') AS DT_ADM,
    IF(
        date_diff('month', US.USUA_DT_ADMISS, current_date) >= 12,
        date_diff('month', US.USUA_DT_ADMISS, current_date) / 12,
        date_diff('month', US.USUA_DT_ADMISS, current_date)
    ) AS TEMPO,
    IF(
        date_diff('month', US.USUA_DT_ADMISS, current_date) >= 12,
        'ANO',
        'MESES'
    ) AS UN_TEMPO,
    month(US.USUA_DT_ADMISS) AS MES_ADM,
    date_format(US.USUA_DT_NASCIM, '%d/%m/%Y') AS DT_NASC,
    date_diff('year', US.USUA_DT_NASCIM, current_date) AS "IDADE(ANOS)",
    US.USUA_DS_EMAIL,
    US.USUA_TP_SEXO,
    US.USUA_DT_RESCIS

FROM modelled.cosmos_v14b_dbo_usuario US
INNER JOIN modelled.cosmos_v14b_dbo_CARGO CG ON CG.CARG_CD_CARGO = US.CARG_CD_CARGO
INNER JOIN modelled.cosmos_v14b_dbo_LOTACAO_USUARIO LT ON LT.LTUS_CD_LOT_USU = US.LTUS_CD_LOT_USU
--LEFT JOIN modelled.cosmos_v14b_dbo_VW_CpfFuncionario CPF ON CPF.MATRICULA = US.USUA_CD_USUARIO

ORDER BY US.USUA_CD_USUARIO DESC;


