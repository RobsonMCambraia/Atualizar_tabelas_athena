create table prevencao_a_fraude.vendas_cambraia as (
with CTE_VENDAS as (
	SELECT
		date_format(cab.mvvc_dt_mov, '%d/%m/%Y') as DATA_VDA,
	    CAST(cab.mvvc_cd_filial_mov AS VARCHAR) AS FILIAL,
		SUBSTRING(cab.mvvc_dt_hr_abe_sis, 9, 2) || ':' || 
		SUBSTRING(cab.mvvc_dt_hr_abe_sis, 11, 2) || ':' || 
		SUBSTRING(cab.mvvc_dt_hr_abe_sis, 13, 2) as HH_ABE,
		SUBSTRING(cab.mvvc_dt_hr_fec_sis, 9, 2) || ':' || 
		SUBSTRING(cab.mvvc_dt_hr_fec_sis, 11, 2) || ':' || 
		SUBSTRING(cab.mvvc_dt_hr_fec_sis, 13, 2) as HH_FEC,
		PRD.CODIGO_PRE_VENDA as ID_PV,
		CAST(cab.numero_coo AS VARCHAR) AS COO,
		cast(CAB.MVVC_NR_CPM_ECF as VARCHAR) as CUPOM,	
	    CAST(cab.mvvc_nr_ecf_mov AS VARCHAR) AS ECF,
	    CAST(opr.stop_cd_usu_opr AS VARCHAR) AS OP_CX,
	    CAST(opcx.usua_nm_usuario AS VARCHAR) AS NM_OPR,
	    CAST(prd.mvvp_nr_prd AS VARCHAR) AS COD_PRD,
	    CAST(prd.mvvp_qt_prd AS INTEGER) AS QTD_PRD,
	    CAST(pm.prme_tx_descricao1 AS VARCHAR) AS DESCRICAO,
		CAST(prd.mvvp_vl_prd_ven AS DECIMAL(10, 2)) AS VALOR,
        CAST(cab.mvvc_vl_tot_cpm AS DECIMAL(10, 2)) AS TOTAL_CPM,
		  CASE
		    WHEN(FLZ.FLVD_TP_POS) = 'A' THEN 'TEF'
		    WHEN(FLZ.FLVD_TP_POS) = 'M' THEN 'POS'
		    ELSE 'OUTRO'
		  END AS NM_FINALIZ,
		  CASE
		    WHEN(FLZ.FLVD_CD_FLZ) = 1 THEN 'CASH'
		    WHEN(FLZ.FLVD_CD_FLZ) = 2 THEN 'CARTAO'
		    WHEN(FLZ.FLVD_CD_FLZ) = 3 THEN 'CHEQUE'
		    ELSE 'OUTRO'
		  END AS NAT_FINALIZ,
	    CAB.mvvc_st_can_cpm AS CUPOM_CANC,
	    PRD.MVVP_ST_CAN_ITE AS ITEM_CANC,
	    CAST(cab.mvvc_cd_usu_t052_can AS VARCHAR) AS AUTORIZADOR,
	    CAST(us.usua_nm_usuario AS VARCHAR) AS NM_AUT,
	    CAST(cab.chave_acesso_nfce AS VARCHAR) AS CHAVE_NFCE,
	    CAST(cab.nome_cliente_sempre AS VARCHAR) AS NOME_CLIENTE
	FROM
	    modelled.cosmos_mov_v14b_dbo_mv_vda_cab cab
	LEFT JOIN 
	    modelled.cosmos_mov_v14b_dbo_mv_vda_prd prd 
	    ON cab.mvvc_nr_ecf_mov = prd.mvvp_nr_ecf_mov 
	    AND cab.mvvc_cd_filial_mov = prd.mvvp_cd_filial_mov 
	    AND cab.mvvc_dt_mov = prd.mvvp_dt_mov 
	    AND cab.mvvc_ct_vda = prd.mvvp_ct_vda
	INNER JOIN
	    modelled.cosmos_v14b_dbo_produto_mestre pm 
	    ON prd.mvvp_nr_prd = pm.prme_cd_produto
	INNER JOIN
	    modelled.cosmosmov_v14b_dbo_st_opr opr 
	    ON opr.stop_ct_st_opr = cab.mvvc_ct_st_opr 
	    AND opr.stop_nr_ecf_mov = cab.mvvc_nr_ecf_mov 
	    AND opr.stop_cd_filial_mov = cab.mvvc_cd_filial_mov 
	    AND opr.stop_dt_mov = cab.mvvc_dt_mov
	INNER JOIN
	    modelled.cosmos_v14b_dbo_filial fil 
	    ON fil.fili_cd_filial = cab.mvvc_cd_filial_mov
	INNER JOIN
	    modelled.cosmos_v14b_dbo_usuario opcx 
	    ON opr.stop_cd_usu_opr = opcx.usua_cd_usuario
	LEFT JOIN
	    modelled.cosmos_v14b_dbo_usuario as us 
	    ON cab.mvvc_cd_usu_t052_can = us.usua_cd_usuario
	LEFT JOIN     
	    modelled.cosmosfl_dbo_flz_vda flz 
	    ON flz.flvd_ct_vda = cab.mvvc_ct_vda 
	    AND flz.flvd_cd_filial_mov = cab.mvvc_cd_filial_mov 
	    AND flz.flvd_dt_mov = cab.mvvc_dt_mov
	    
	WHERE cab.mvvc_dt_mov >= date_add('day', -60, current_date)
    )
select 
	CV.DATA_VDA,
	CV.FILIAL,
	cast(substring(CV.HH_ABE, 1, 2) as INTEGER) AS FX_HH,
	CV.HH_ABE,
	CV.HH_FEC,
	CV.ID_PV,
	CV.COO,
	CV.CUPOM,
	CV.ECF,
	CV.OP_CX,
	CV.NM_OPR,
	CV.COD_PRD,
	CV.QTD_PRD,
	CV.DESCRICAO,
	CV.VALOR,
	CV.TOTAL_CPM,
	CV.NM_FINALIZ,
	CV.NAT_FINALIZ,
	CV.CUPOM_CANC,
	CV.ITEM_CANC,
	CV.AUTORIZADOR,
	CV.NM_AUT,
	CV.CHAVE_NFCE,
	CV.NOME_CLIENTE
from CTE_VENDAS CV
order by CV. FILIAL, CV.DATA_VDA, CV.COO asc);