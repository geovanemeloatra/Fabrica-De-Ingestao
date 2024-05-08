let parameters = {

   format: 'bigquery',
   table_name: 'ewally_adquirencia_estabelecimento',
   source_table_name: 'ewally_adquirencia_estabelecimento_mssql_inc',
   source_dataset: 'staging',
   developer: ['matheus_felippe_silva_ext@carrefour.com'],
   table_description: 'Tabela usada para mostrar todas as informações do estabelecimento para adquirencia e o MCC  Expurgo: não necessário por ser uma tabela de domínio.  Origem: TSTG_ADQUIRC_ESTABELECIMENTO',
   partition_by: {column: 'ingestion_timestamp', type: 'timestamp', granularity: 'day'},
   cluster_by: ["ingestion_ref_date"],
   ingestion_ref_date_column: "FORMAT_DATE('%Y%m%d', dh_criac)",
   custom_tags: ['PIT0070'],
   pec: 70,
   pec_obs: 'PIT0070 - Ewally',

   columns: [
      {name: 'id_estabelecimento', type: 'NUMERIC'},
      {name: 'nm_estabecimento', type: 'STRING'},
      {name: 'nu_docestabelecimento', type: 'STRING'},
      {name: 'vl_faturamentomensal', type: 'NUMERIC'},
      {name: 'id_usucriac', type: 'STRING'},
      {name: 'dh_criac', type: 'DATE'},
      {name: 'id_usualtregis', type: 'STRING'},
      {name: 'dh_altregis', type: 'DATETIME'},
      {name: 'id_mcc', type: 'NUMERIC'},
      {name: 'id_adquirente', type: 'NUMERIC'},
      {name: 'id_grupo', type: 'NUMERIC'},
      {name: 'cd_contadigital', type: 'STRING'},
      {name: 'fg_approvaladquirencia', type: 'BOOL'},
      {name: 'id_estabelecimento_bloqueio', type: 'NUMERIC'},
      {name: 'fg_bloqueado', type: 'BOOL'},
      {name: 'fg_primeiroacesso', type: 'BOOL'},
   ],

   columns_description : {
      id_estabelecimento: 'Identificador da tabela de estabelecimento.',
      nm_estabecimento: 'Nome estabelecimento.',
      nu_docestabelecimento: 'Número do documento do estabelecimento.',
      vl_faturamentomensal: 'Valor do faturamento comercial.',
      id_usucriac: 'Identificador do usuário de criação.',
      dh_criac: 'Identificador da data de criação.',
      id_usualtregis: 'Identificador do usuário de alteração.',
      dh_altregis: 'Data de alteração.',
      id_mcc: 'Codigo do MCC.',
      id_adquirente: 'Identificador da tabela de adquirente.',
      id_grupo: 'Identificador único do registro, gerado automaticamente pelo banco de dados (campo identity do SQL).',
      cd_contadigital: 'Código da conta digital, utilizado nas transações com a EWally. Origem: campo ACCOUNT_NAME na API da EWally, gerado na criação da conta digital Exemplo: 62356230513878_89276564071, sendo [CONTAMESTRE_CPFUSUARIO].',
      fg_approvaladquirencia: 'Flag aprova valor adquirencia.',
      id_estabelecimento_bloqueio: 'Identificador do estabelecimento bloqueado.',
      fg_bloqueado: 'Flag bloqueado.',
      fg_primeiroacesso: 'Flag do primeiro acesso.',
   },

   policy_tags: {
      nm_estabecimento: 'pii',
      nu_docestabelecimento: 'pii',
      cd_contadigital: 'pii'
   },

   data_governance: [
      {name: "schedule_name", value: "P0070IT_ewally_adquirencia_estabelecimento"},
      {name: "data_classification", value: "Internal"},
      {name: "partitioned_table", value: "true"},
      {name: "clustered_table", value: "true"},
      {name: "update_frequency", value: "Daily"},
      {name: "load_type", value: "Incremental"},
      {name: "data_retention", value: "Not Applicable"}
   ]

}

csf_modules.ingestion.ingest(parameters)
