let parameters = {
  format: 'CSV',
  table_name: 'tsys_cpaymentslips',
  table_description: 'Tabela dados transacionais da TSYS',
  cluster_by: ["ingestion_ref_date"],
  developer: ['kleber_albuquerque_de_lima_ext@carrefour.com','carlos_eduardo_rocha_araujo_ext@carrefour.com'],
  pec: 2,
  custom_tags:['PIT0002'],
  pec_obs: 'PIT 0002 - TSYS',
  field_delimiter: '|',
  skip_leading_rows: 0,
  allow_jagged_rows: true,
  ignore_unknown_values: true,
  partition_by: {column: 'ingestion_timestamp', type: 'timestamp', granularity: 'day'},
  data_governance: [
      {name: "schedule_name", value: "tsys_cpaymentslips"}
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
    , {name: 'caccserno', type: 'numeric'}
    , {name: 'stmtserno', type: 'numeric'}
    , {name: 'outbatchserno', type: 'numeric'}
    , {name: 'createddate', type: 'numeric'}
    , {name: 'duedate', type: 'numeric'}
    , {name: 'amount', type: 'numeric'}
    , {name: 'totalpayments', type: 'numeric'}
    , {name: 'lastactiondate', type: 'numeric'}
    , {name: 'lastaction', type: 'string'}
    , {name: 'status', type: 'numeric'}
    , {name: 'paymentcount', type: 'numeric'}
    , {name: 'type_fg', type: 'numeric'}
    , {name: 'accountnumber', type: 'numeric'}
    , {name: 'portfolio', type: 'numeric'}
  ],

  columns_description : {
      type:'The type of record'
    , institution_id:'Institution ID for implementing multiple institutions'
    , serno:'Unique permanent serial number'
    , partitionkey:'Partition key used for partitioning'
    , caccserno:'Foreign key reference to caccounts.serno, index.'
    , stmtserno:'Foreign key reference to cstatements.serno, index.'
    , outbatchserno:'Foreign key reference to batches.serno, filled when outgoing CNAB file is generated.'
    , createddate:'Date when the payment slip was generated'
    , duedate:'Due date of payment slip, for statements generation the value will be the due date of the statement, for ad-hoc request the value will be defined by the user.'
    , amount:'Amount of the payment slip. '
    , totalpayments:'Sum of all payments made for the payment slip'
    , lastactiondate:'Date of the last payment received'
    , lastaction:'Last action of payment slip, initial value “insert”'
    , status:'Flag indicative of status'
    , paymentcount:'Count of all payments made for the payment slip'
    , type_fg:'Indicates the type of the payment slip'
    , accountnumber:'Number of the account that is linked to this payment slip'
    , portfolio:'Optional. If set, will override the portfolio at the product level'
  }
}

csf_modules.ingestion.ingest(parameters)
