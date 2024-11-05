let parameters = {
    format: 'bigquery',
    table_name: 'gf_creditopessoal_proposta',
    table_description: 'Tabela com informações do GF (Gestão Financeira), utilizada exclusivamente pela Squad de Produtos Financeiros.',
    source_table_name: 'gf_creditopessoal_proposta',
    source_dataset: 'staging',
    developer: ['guilherme_barros_alves_pereira_ext@carrefour.com'],
    pec: 36,
    custom_tags: ['PIT0036'],
    pec_obs: 'PIT 0036 - TSYS',
    partition_by: {column: 'ingestion_timestamp', type: 'timestamp', granularity: 'day'},
    ingestion_ref_date_column: 'CAST(FORMAT_DATE("%Y%m%d", IFNULL(dh_alt, dh_criac)) AS INT64)',
    cluster_by: ["ingestion_ref_date"],
    declare_source_table: false,
  
    data_governance: [
        {name: "schedule_name", value: "P0036I_gf_creditopessoal_proposta"}
      , {name: "data_classification", value: "Internal"}
      , {name: "partitioned_table", value: "true"}
      , {name: "clustered_table", value: "true"}
      , {name: "update_frequency", value: "Daily"}
      , {name: "load_type", value: "Incremental"}
      , {name: "data_retention", value: "Not Applicable"}
    ],
    
    data_governance_column: [
        {column:'accountnumber', name:'pii', value:'true'}
    ],
    policy_tags: {
        accountnumber: 'pii'
    },
    
    columns: [
        {name: 'type', type: 'string'}
      , {name: 'institution_id', type: 'numeric'}
      , {name: 'serno', type: 'numeric'}
      , {name: 'partitionkey', type: 'numeric'}
    ],
  
    columns_description : {
        type: 'The type of record'
      , institution_id: 'Institution ID for implementing multiple institutions'
      , serno: 'Unique permanent serial number'
      , partitionkey: 'Partition key used for partitioning'
    }
}
  
csf_modules.ingestion.ingest(parameters)
  