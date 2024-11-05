let params = {
    type: 'table',
    object_name: 'ewally_conta_digital_pix_keys_mssql_inc',
    object_description: 'dadasdasdasdadaddsadasd',
    dataset: 'staging',
    pec: 70,
    pec_obs: 'PIT0070 - Ewally',
    custom_tags:['PIT0070'],
    developer: ['guilherme_barros_alves_pereira_ext@carrefour.com'],
    data_governance: [
        {name: "schedule_name", value: "Not Applicable"},
        {name: "data_classification", value: "Confidential"},
        {name: "partitioned_table", value: "false"},
        {name: "clustered_table", value: "false"},
        {name: "update_frequency", value: "Daily"},
        {name: "load_type", value: "Incremental"},
        {name: "data_retention", value: "Not Applicable"}
    ],
    data_governance_column: [
        {column: 'KEYVALUE', name: 'pii', value: 'true'},
    ],
    policy_tags: {
        KEYVALUE: 'pii',
    },
   columns_description : {
      ID: 'Identificador da chave PIX',
   },

}

csf_modules.transformation
.create_new_object(params)
.query(ctx => `

    WITH table_layout AS (

        SELECT
            array<struct<
                ID STRING
                , ID STRING
            >>

        [] AS array_columns

    )

    SELECT
        ID
        , ID
    FROM
        table_layout,
    UNNEST
        (table_layout.array_columns)

`)
