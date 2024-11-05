let params = {
    type: "upsert",
    source_object : "appcartao_tb_cartaovirtual_controle",
    source_dataset: "raw",
    dataset: "trusted",
    object_name: "appcartao_tb_cartaovirtual_controle",
    object_description: 'Tabela criada para armazenar as transacoes de criação de Cartoes Virtuais - Capacity - Previsao de 300.000 registros por ano.',
    key_columns: ["id_ctrlcartaovirt"],
    watermark_columns: ["ingestion_ref_date"],
    incremental_watermark: "ingestion_timestamp",
    partition_by: {column: "ingestion_timestamp", type: "timestamp", granularity: "day"},
    cluster_by: ["ingestion_ref_date"],
    developer: ["guilherme_barros_alves_pereira_ext@carrefour.com"],
    pec: 122,
    pec_obs: 'PIT00122 - App Cartão',
    custom_tags: ['PIT0122'],
    columns: ['*'],

    data_governance_column: [
        {column:'nu_cartao', name:'pii', value:'true'},
      , {column:'nu_cartaovirt', name:'pii', value:'true'}
      , {column:'nu_conta', name:'pii', value:'true'}
    ],
    policy_tags: {
        nu_cartao: 'pii'
      , nu_cartaovirt: 'pii'
      , nu_conta: 'pii'
    },
  
    columns_description : {
        id_ctrlcartaovirt: 'Identificador de controle do Cartao Virtual'
      , nu_cartao: 'Numero criptografado do Cartao Fisico'
      , nu_cartaovirt: 'Numero criptografado do Cartao Virtual.'
      , dh_geraccartaovirt: 'Data / hora de geracao do cartao virtual'
      , dt_expir: 'Data de expiracao criptografada do Cartao Virtual'
      , fg_excl: 'Flag para exclusao logica do Cartao Virtual'
      , tp_cartaovirt: 'Tipo de cartão virtual'
      , cd_canal: 'Código do canal'
      , nu_conta: 'Número da conta corrente'
      , cd_template: ''
      , nu_cartaovirtmasc: ''
      , dh_exclusao: 'Data / hora de exclusão do cartao virtual'
    }, 
    data_governance: [
        {name: "schedule_name", value: "P0122I_appcartao_tb_cartaovirtual_controle"},
        {name: "data_classification", value: "Internal"},
        {name: "partitioned_table", value: "true"},
        {name: "clustered_table", value: "true"},
        {name: "update_frequency", value: "Daily"},
        {name: "load_type", value: "Incremental"},
        {name: "data_retention", value: "Not Applicable"}
    ]
}
  
  
csf_modules.deduplication.dedup(params)
  
  