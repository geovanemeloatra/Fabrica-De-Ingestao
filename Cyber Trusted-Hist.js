let params = {
    type: "incremental",
    dataset: "trusted",
    object_name: 'cybr_que_dectbl_hist',
    object_description:"A tabela salva a rede de decisao para asinar contas nas filas do trabalho. Tem a mesma estrutura que as outras redes (agn_dectbl, etc.). Esta informacao lee o programa beb115, responsavel a segmentar a carteira",
    partition_by: {column: 'ingestion_timestamp', type: 'timestamp', granularity: 'day'},
    unique_key: [ "ACCT_GRP", "LEVEL1", "TEST", "SEQUENCE" ,"ingestion_ref_date"],
    cluster_by: ["ingestion_ref_date"],
    pec: 3,
    pec_obs: 'PIT 0003 - Cyber',  
    custom_tags:['PIT0003'],
    developer: [ "cynthia_lorena_mendoza_ext@carrefour.com" ],
    pre_ops_declaration: `DECLARE P_MAX_TIMESTAMP TIMESTAMP DEFAULT(select COALESCE(max(ingestion_timestamp), TIMESTAMP ('1900-01-01 00:00:00+00:00') ) from \`${dataform.projectConfig.defaultDatabase}.trusted.cybr_que_dectbl_hist\`);`,
    columns: ["*"] ,

    policy_tags: {
    },

    data_governance: [
		{name: "schedule_name", value: "cybr_que_dectbl"},
		{name: "data_classification", value: "Internal"},
		{name: "partitioned_table", value: "true"},
		{name: "clustered_table", value: "true"},
		{name: "update_frequency", value: "Daily"},
		{name: "load_type", value: "Incremental"},
		{name: "data_retention", value: "Not Applicable"}
    ],

    columns_description : { 
        dh_ref: 'Data em que a alteracao do registro ocorreu no Cyber', 
        in_oper: 'Indicador da alteracao realizada no registro ("I" = Inclusao', 
        serial: 'Ordem que deve ser seguida para gravar os dados na ods.', 
        acct_grp: 'Codigo do Grupo da Conta Parte da "Primary Key" (logica) da tabela.', 
        level1: 'Nivel para esta prova. Parte da "Primary Key" (logica) da tabela.', 
        test: 'Numero de provas dentro de um nivel. Parte da "Primary Key" (logica) da tabela.', 
        sequence: 'Numero sequencial dentro de uma prova. Parte da "Primary Key" (logica) da tabela.', 
        next_level: 'Nivel ao que se avanca se a prova e verdadeiro.', 
        comment1: 'Comentario da prova (so para sequence = 0).', 
        op: 'Operador de comparacao (so para sequence = 1 a 999). Os operadores validos sao', 
        field_1: 'Campo mestre que se compara usando o operador.', 
        field_2: 'Um dos campos a comparar.', 
        field_2_flag: 'Indica se o dado 2 e um campo ou um valor.', 
        field_3: 'Um dos campos a comparar.', 
        field_3_flag: 'Indica se o dado 3 e um campo ou um valor.', 
        or_flag: 'Bandeira usada para agrupar varias comparacoes. So tem dois possiveis valores', 
        num_return: 'Numero de valores regressados (so para sequence = 0).', 
        nxt_return: 'RESERVADO.', 
        queue: 'Valores de regresso (para sequence = 10000 a 32767).', 
    },

    
}
 csf_modules.transformation
    .create_new_object(params)
    .query (ctx => `
WITH 
temp_processar AS ( -- temporaria com os sernos que devem ser processados
  SELECT DISTINCT ACCT_GRP, LEVEL1, TEST, SEQUENCE
    FROM ${ctx.ref("raw", "cybr_que_dectbl")} 
    ${ctx.when(ctx.incremental(),` WHERE ingestion_timestamp > P_MAX_TIMESTAMP`)}

),
-- dados clean que precisam ser processados: contem novos registros e registros com vigencia que precisa ser fechada
-- resultado para o merge na hist, comparando com a chave  + refdate (inicio da vigencia)
--Temp distinct filtra os dados duplicados por ter esse comportamento o arquivo ou por processar dois vezes o mesmo arquivo.
--consideramos so essa chave para asegurar que o comportamento do arquivo seja o esperado

temp_distinct AS (
    SELECT *
	FROM ${ctx.ref("raw", "cybr_que_dectbl")} t1 
	WHERE EXISTS (SELECT 1 from temp_processar t2  
	              where t2.ACCT_GRP = t1.ACCT_GRP AND 
                      t2.LEVEL1 = t1.LEVEL1 AND
                      t2.TEST = t1.TEST AND
                      t2.SEQUENCE = t1.SEQUENCE )
	QUALIFY ROW_NUMBER() OVER(PARTITION BY ACCT_GRP, LEVEL1, TEST, SEQUENCE, ingestion_ref_date
                              ORDER BY ingestion_timestamp desc) = 1
) 

SELECT  parse_date('%Y%m%d',CAST(ingestion_ref_date AS string)) AS dat_inicio_vigencia,
        lead(parse_date('%Y%m%d',CAST(ingestion_ref_date as string))) OVER (PARTITION BY ACCT_GRP,LEVEL1,TEST,SEQUENCE ORDER BY ingestion_ref_Date ASC) as dat_fim_vigencia,
        c.dh_ref
        , CASE WHEN trim(c.in_oper)='' THEN NULL ELSE in_oper END in_oper
        , C.serial
        , CASE WHEN trim(c.acct_grp)='' THEN NULL ELSE acct_grp END acct_grp
        , c.level1
        , c.test
        , c.sequence
        , c.next_level
        , CASE WHEN trim(c.comment1) ='' THEN NULL ELSE comment1 END comment1
        , CASE WHEN trim(c.op) ='' THEN NULL ELSE op END op
        , CASE WHEN trim(c.field_1) ='' THEN NULL ELSE field_1 END field_1 
        , CASE WHEN trim(c.field_2) ='' THEN NULL ELSE field_2 END field_2 
        , CASE WHEN trim(c.field_2_flag) ='' THEN NULL ELSE field_2_flag END field_2_flag
        , CASE WHEN trim(c.field_3) ='' THEN NULL ELSE field_3 END field_3
        , CASE WHEN trim(c.field_3_flag) ='' THEN NULL ELSE field_3_flag END field_3_flag 
        , CASE WHEN trim(c.or_flag) ='' THEN NULL ELSE or_flag END or_flag
        , c.num_return
        , c.nxt_return
        , CASE WHEN trim(trim(c.queue)) = '' THEN NULL ELSE queue END queue
        , c.ingestion_timestamp
        , c.ingestion_source_file
        , c.ingestion_proc_datetime
        , c.ingestion_ref_date
FROM temp_distinct c

`
);
