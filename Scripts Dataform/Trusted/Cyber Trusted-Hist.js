let params = {
    type: "incremental",
    dataset: "trusted",
    object_name: 'cybr_lbl_dectbl1_hist',
    object_description: "Arvore de decisao para qualificar creditos.",
    partition_by: {column: 'ingestion_timestamp', type: 'timestamp', granularity: 'day'},
    cluster_by: ["ingestion_ref_date"],
    unique_key: ["acct_grp", "level1", "test", "sequence", "ingestion_ref_date", "serial"],
    pec: 3,
    pec_obs: 'PIT 0003 - CYBER',
    custom_tags: ['PIT0003'],
    developer: ['guilherme_barros_alves_pereira_ext@carrefour.com'],
    pre_ops_declaration: `DECLARE P_MAX_TIMESTAMP TIMESTAMP DEFAULT (SELECT COALESCE(MAX(ingestion_timestamp), TIMESTAMP('1900-01-01')) FROM \`${dataform.projectConfig.defaultDatabase}.trusted.cybr_lbl_dectbl1_hist\`);`,
    columns: ["*"] ,

    data_governance_column: [
        {column:'accountnumber', name:'pii', value:'true'}
    ],
    
    policy_tags: {
        accountnumber: 'pii'
    },
    
    data_governance: [
          {name: "schedule_name", value: "cybr_lbl_dectbl1"}
        , {name: "data_classification", value: "Internal"}
        , {name: "partitioned_table", value: "true"}
        , {name: "clustered_table", value: "true"}
        , {name: "update_frequency", value: "Daily"}
        , {name: "load_type", value: "Incremental"}
        , {name: "data_retention", value: "Not Applicable"}
    ],

    columns_description: {
        dh_ref: 'Data em que a alteracao do registro ocorreu no Cyber',
        in_oper: 'Indicador da alteracao realizada no registro ("I" = Inclusao, "A" = Alteracao, "E" = Exclusao)',
        serial: 'Ordem que deve ser seguida para gravar os dados na ods.',
        acct_grp: 'Codigo do Grupo da Conta Parte da "Primary Key" (logica) da tabela.',
        level1: 'Nivel para esta prova. Parte da "Primary Key" (logica) da tabela.',
        test: 'Numero de provas dentro de um nivel. Parte da "Primary Key" (logica) da tabela.',
        sequence: 'Numero sequencial dentro de uma prova. Parte da "Primary Key" (logica) da tabela.',
        next_level: 'Nivel ao que se avanca se a prova e verdadeiro.',
        comment1: 'Comentario da prova (so para sequence = 0).',
        op: 'Operador de comparacao (so para sequence = 1 a 999). Os operadores validos sao: .eo. Igual a dv1 ou igual a dv2. Exemplo: c = dv1 ou c = dv2. .eq. Igual a. Exemplo: c = dv1. .ge. Maior ou igual a. Exemplo: c >= dv1. .gt. Maior a. Exemplo: c > dv1. .le. Menor ou igual a. Exemplo: c <= dv1. .lt. Menor a. Exemplo: c < dv1. .ne. Diferente de. Exemplo: c!= dv1. .no. Diferente a e diferente a. Exemplo: c!= dv1 e c!= dv2. .os. Menor a ou Maior a. Exemplo: c < dv1 ou c > dv2. .we. Maior ou igual a e Menor ou igual a (faixa de valores). Ex.: c >= dv1 e c <= dv2.',
        field_1: 'Campo mestre que se compara usando o operador.',
        field_2: 'Um dos campos a comparar.',
        field_2_flag: 'Indica se o dado 2 e um campo ou um valor.',
        field_3: 'Um dos campos a comparar.',
        field_3_flag: 'Indica se o dado 3 e um campo ou um valor.',
        or_flag: 'Bandeira usada para agrupar varias comparacoes. So tem dois possiveis valores: branco = indica que nao se agrupam as comparacoes. 0 = indica que as comparacoes se agrupam.',
        num_return: 'Numero de valores regressados (so para sequence = 0).',
        nxt_return: 'RESERVADO.',
        queue: 'Valores de regresso (para sequence = 10000 a 32767).'
    },
}

csf_modules.transformation.create_new_object(params).query (ctx => `

-- LISTA TEMPORÁRIA DAS CHAVES CARREGADAS NA CAMADA RAW

WITH LISTA_DE_CHAVES_CARREGADAS AS (

    SELECT DISTINCT acct_grp
    FROM ${ctx.ref("raw", "cybr_lbl_dectbl1")}
    ${ctx.when(ctx.incremental(), `WHERE ingestion_timestamp > P_MAX_TIMESTAMP`)}

)

-- RETORNO COM TODAS AS LINHAS A SEREM PROCESSADAS (INCLUINDO NOVOS REGISTROS E REGISTROS COM VIGÊNCIA A SER FECHADA)

SELECT
      PARSE_DATE('%Y%m%d', CAST(ingestion_ref_date AS STRING)) AS dat_inicio_vigencia
    , LEAD(PARSE_DATE('%Y%m%d', CAST(ingestion_ref_date AS STRING))) OVER(PARTITION BY acct_grp ORDER BY ingestion_ref_date, ingestion_timestamp) AS dat_fim_vigencia
    , dh_ref
    , CASE WHEN TRIM(in_oper) = "" THEN NULL ELSE in_oper END in_oper
    , serial
    , CASE WHEN TRIM(acct_grp) = "" THEN NULL ELSE acct_grp END acct_grp
    , level1
    , test
    , sequence
    , next_level
    , CASE WHEN TRIM(comment1) = "" THEN NULL ELSE comment1 END comment1
    , CASE WHEN TRIM(op) = "" THEN NULL ELSE op END op
    , CASE WHEN TRIM(field_1) = "" THEN NULL ELSE field_1 END field_1
    , CASE WHEN TRIM(field_2) = "" THEN NULL ELSE field_2 END field_2
    , CASE WHEN TRIM(field_2_flag) = "" THEN NULL ELSE field_2_flag END field_2_flag
    , CASE WHEN TRIM(field_3) = "" THEN NULL ELSE field_3 END field_3
    , CASE WHEN TRIM(field_3_flag) = "" THEN NULL ELSE field_3_flag END field_3_flag
    , CASE WHEN TRIM(or_flag) = "" THEN NULL ELSE or_flag END or_flag
    , num_return
    , nxt_return
    , CASE WHEN TRIM(queue) = "" THEN NULL ELSE queue END queue
    , ingestion_timestamp
    , ingestion_source_file
    , ingestion_proc_datetime
    , ingestion_ref_date
FROM
  ${ctx.ref("raw", "cybr_lbl_dectbl1")} AS TB
WHERE
  EXISTS (SELECT 1 FROM LISTA_DE_CHAVES_CARREGADAS AS LISTA WHERE LISTA.acct_grp = TB.acct_grp)

`);
