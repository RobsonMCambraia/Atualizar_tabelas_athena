create table prevencao_a_fraude.qtd_falta as
SELECT
    cast(KF.KAFI_CD_FILIAL as varchar) as KAFI_CD_FILIAL,
    cast(KF.KAFI_CD_PRODUTO as varchar) as KAFI_CD_PRODUTO,
    KF.KAFI_TP_MOV,
    SUM(CAST(KF.KAFI_QT_MOV AS integer)) AS QTD
FROM 
    modelled.cosmos_v14b_dbo_kardex_filial KF
WHERE
    CAST(KF.KAFI_DH_OCORRREAL AS TIMESTAMP) >= date_add('day', -10, current_date)
    AND KF.KAFI_TP_MOV IN ('SA', 'S9')
    AND KF.KAFI_TX_NR_DOCTO LIKE '%/%'
GROUP BY
    KF.KAFI_CD_FILIAL,
    KF.KAFI_CD_PRODUTO,
    KF.KAFI_TP_MOV