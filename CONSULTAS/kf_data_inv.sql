create table prevencao_a_fraude.data_inventario as
SELECT
    date_format(MAX(CAST(KF.KAFI_DH_OCORRREAL AS TIMESTAMP)), '%d/%m/%Y') AS DATA_INVENTARIO,
    CAST(KF.KAFI_CD_FILIAL AS VARCHAR) AS KAFI_CD_FILIAL
FROM 
    modelled.cosmos_v14b_dbo_kardex_filial KF
WHERE
    CAST(KF.KAFI_DH_OCORRREAL AS TIMESTAMP) >= date_add('day', -10, current_date)
    AND KF.KAFI_TX_NR_DOCTO LIKE '%/%'
GROUP BY
    KF.KAFI_CD_FILIAL