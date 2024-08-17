select 
    cast(prd.mvvp_nr_prd as varchar) as codigointerno,
    cast(prd.codigo_ean as varchar) as cod,
    cast(pm.prme_tx_descricao1 as varchar) as descricao,
    cast(prd.mvvp_vl_pre_vda as decimal(18, 2)) as de,
    cast(prd.mvvp_vl_prd_ven as decimal(18, 2)) as por,
    cast(sum(prd.mvvp_qt_prd) as integer) as quantidadeunidade,        
    cast(prd.preco_ref_prod as decimal(18, 2)) as valorunidade,
    cast(sum(prd.mvvp_vl_pre_vda) as decimal(18, 2)) as valoritem,
    cast(sum(prd.mvvp_vl_dsc_ite) as decimal(18, 2)) * -1 as descontosobreitem,
    cast(sum(prd.mvvp_qt_prd) over (partition by cab.numero_coo) as integer) as quantidadeitem,
    cast(sum(sum(prd.mvvp_vl_pre_vda)) over (partition by cab.numero_coo) as decimal(18, 2)) as totalbrutoitem,
    cast(sum(sum(prd.mvvp_vl_dsc_ite)) over (partition by cab.numero_coo) as decimal(18, 2)) * -1 as totaldescontoitem,
    cast(cab.mvvc_vl_tot_cpm as decimal(18, 2)) as valortotal,
    cast(cab.mvvc_dt_mov as date) as datamov,
    cast(cab.mvvc_cd_filial_mov as varchar) as loja,
    cast(cab.mvvc_nr_ecf_mov as varchar) as terminal,
    cast(cab.numero_coo as varchar) as ncupom,
    cast(cab.chave_acesso_nfce as varchar) as chave_acesso_nfce,
    cab.data_hora_emissao_nfce_sat,
    '--' as "--",
    concat(date_format(cab.mvvc_dt_mov, '%Y%m%d'), cast(cab.mvvc_cd_filial_mov as varchar), cast(cab.mvvc_nr_ecf_mov as varchar), cast(cab.numero_coo as varchar)) as chave,
    cast(sum(sum(prd.mvvp_qt_prd)) over (partition by cab.numero_coo) as integer) as unidade,
    cast(cab.mvvc_nr_ecf_mov as varchar) as pdv,
    cast(cab.mvvc_cd_filial_mov as varchar) as lj,
    cast(cab.numero_coo as varchar) as coo,
    cast(opr.stop_cd_usu_opr as varchar) as op,
    cast(operador.usua_nm_usuario as varchar) as nomeop,
    cast(prd.mvvp_cd_usuario as varchar) as vendedor,
    cast(vendedor.usua_nm_usuario as varchar) as nomevendedor,
    '--' as "---",
    cast(opr.stop_cd_usu_opr as varchar) as matriculaoperador,
    cast(operador.usua_nm_usuario as varchar) as nomeoperador,
    cast(cab.mvvc_cd_usu_t052_can as varchar) as matriculaautorizador,
    cast(autorizador.usua_nm_usuario as varchar) as nomeautorizador,
    cab.mvvc_st_can_cpm,
    prd.mvvp_st_can_ite,
    substring(cab.mvvc_dt_hr_abe_sis, 9, 2) || ':' || substring(cab.mvvc_dt_hr_abe_sis, 11, 2) || ':' || substring(cab.mvvc_dt_hr_abe_sis, 13, 2) as hora_abertura_sistema,
    substring(cab.mvvc_dt_hr_fec_sis, 9, 2) || ':' || substring(cab.mvvc_dt_hr_fec_sis, 11, 2) || ':' || substring(cab.mvvc_dt_hr_fec_sis, 13, 2) as hora_fechamento_sistema,
    prd.codigo_pre_venda,
    cab.nome_cliente,
    cab.cpf_cnpj_cliente_sempre
from modelled.cosmos_mov_v14b_dbo_mv_vda_cab cab
inner join modelled.cosmos_mov_v14b_dbo_mv_vda_prd prd on
    cab.mvvc_nr_ecf_mov = prd.mvvp_nr_ecf_mov and
    cab.mvvc_cd_filial_mov = prd.mvvp_cd_filial_mov and
    cab.mvvc_dt_mov = prd.mvvp_dt_mov and
    cab.mvvc_ct_vda = prd.mvvp_ct_vda
inner join modelled.cosmos_v14b_dbo_produto_mestre pm on
    prd.mvvp_nr_prd = pm.prme_cd_produto
left join modelled.cosmos_v14b_dbo_usuario vendedor on
    prd.mvvp_cd_usuario = vendedor.usua_cd_usuario
inner join modelled.cosmosmov_v14b_dbo_st_opr opr on
    opr.stop_ct_st_opr = cab.mvvc_ct_st_opr and
    opr.stop_nr_ecf_mov = cab.mvvc_nr_ecf_mov and
    opr.stop_cd_filial_mov = cab.mvvc_cd_filial_mov and
    opr.stop_dt_mov = cab.mvvc_dt_mov
inner join modelled.cosmos_v14b_dbo_usuario operador on
    opr.stop_cd_usu_opr = operador.usua_cd_usuario
left join modelled.cosmos_v14b_dbo_usuario autorizador on
    cab.mvvc_cd_usu_t052_can = autorizador.usua_cd_usuario        
where
--    cab.mvvc_cd_filial_mov in (597)                                                       -- filial
--    and cab.mvvc_dt_mov between date '2024-07-17' and date '2024-07-24'                 -- entre a data
     cab.mvvc_dt_mov >= date_add('day', -60, current_date)                           -- maior ou igual a 32 dias atrás
--    and cab.mvvc_dt_mov <= current_date                                                 -- menor ou igual à data e hora atuais
--    and cab.mvvc_dt_mov = date '2024-07-29'
--    and cast(cab.mvvc_vl_tot_cpm as decimal(18, 2)) = cast('1917.27' as decimal(18, 2)) -- valor total cupom
--    and cab.mvvc_cd_usu_t052_can = '73467'                                              -- autorizou cancelamento
--    and prd.usuario_autorizou_desconto_item = ''                                        -- autorizou desconto item
--    and prd.mvvp_pr_dsc_ite = '22.45'                                                   -- percentual de desconto %
--    and cab.numero_coo in (102096)                                                      -- numero coo
--    and regexp_like(trim(pm.prme_tx_descricao1), '.*bude.*')                            -- descriçâo produto
--    or regexp_like(lower(pm.prme_tx_descricao1), '.*bude.*')                            --------------------
group by
    prd.mvvp_nr_prd,
    prd.codigo_ean,
    pm.prme_tx_descricao1,
    prd.mvvp_vl_pre_vda,
    prd.mvvp_vl_prd_ven,
    prd.preco_ref_prod,
    prd.mvvp_qt_prd,
    prd.mvvp_vl_dsc_ite,
    cab.numero_coo,
    cab.mvvc_vl_tot_cpm,
    cab.mvvc_dt_mov,
    cab.mvvc_cd_filial_mov,
    cab.mvvc_nr_ecf_mov,
    cab.data_hora_emissao_nfce_sat,
    cab.chave_acesso_nfce,
    prd.mvvp_cd_usuario,
    cab.mvvc_cd_usu_t052_can,
    operador.usua_nm_usuario,
    vendedor.usua_nm_usuario,
    opr.stop_cd_usu_opr,
    autorizador.usua_nm_usuario,
    cab.mvvc_st_can_cpm,
    prd.mvvp_st_can_ite,
    cab.mvvc_dt_hr_abe_sis,
    cab.mvvc_dt_hr_fec_sis,
    prd.codigo_pre_venda,
    cab.nome_cliente,
    cab.cpf_cnpj_cliente_sempre
order by
    cab.mvvc_dt_hr_fec_sis asc,
    cab.numero_coo asc;