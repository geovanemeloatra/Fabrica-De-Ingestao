let params = {
    type:'table',
    object_name:'tb_dbm_pessoa_fisica_ods_hist',
    object_description: 'Tabela com informações da pessoa fisica - DBM',
    dataset: 'staging',
    pec: 83,
    pec_obs:'PIT0083 - DBM',
    custom_tags:['PIT0083'],
    developer:['geovane_melo_da_silva_ext@carrefour.com'],
columns_description: {
PESS_ID_PESS: 'FK - Identificador da Pessoa. Numero Sequencial',
PEFI_NU_CPF: 'Numero do CPF da PESSOA. Indice pelo CPF para acesso',
PEFI_NM_PESS: 'Nome completo da PESSOA',
PEFI_DT_NASC: 'Data de Nascimento da pessoa',
PEFI_VL_RENDAINFO: 'Valor da Renda da Pessoa Informada',
PEFI_VL_RENDACOMPROV: 'Renda Comprovada',
PEFI_CD_ATIVDEPROFIS: 'Codigo da Atividade Profissional da PESSOA. Natureza da Ocupacao. Exemplos: setor publico, privado, autonomo, aposentado. Os descritivos destes codigos estao na tabela de Domininio.',
PEFI_FG_FUNC: 'Flag de Funcionario. "S" - Sim, "N" - Nao',
PEFI_CD_ESTCIVIL: 'Codigo Estado Civil. Solteiro, Casado, Viuvo',
PEFI_CD_CLASSIFSOC: 'Código da Classificação Social: Este código classifica a classe social com base na renda do cliente. Para funcionários e clientes que não têm o valor de renda informado, o código é definido como nulo. Renda entre 0 e 787: E. Renda entre 788 e 1170: D. Renda entre 1171 e 1927: C2. Renda entre 1928 e 3417: C1. Renda entre 3418 e 6561: B2. Renda entre 6562 e 14483: B1. Renda igual ou superior a 14484: A.',
PEFI_CD_SEXO: 'Código do Sexo: F - Feminino, M - Masculino',
TPRO_CD_PROFIS: 'Profissão: É a função que o cliente exerce ou está exercendo. Pode ser também a profissão. Exemplos: Padeiro, Engenheiro, Dona de Casa, Analista. ForeignKey (FK) da Tabela TB_DBM_PROFISSAO.',
STOR_CD_SISTORI: 'Código Sistema Origem do Registro da Tabela: Exemplo: Tsys, CCI, CYBER',
PEFI_DH_INCLSISTORI: 'Data de Inclusão - Sistema Origem',
PEFI_DH_INCLREGIS: 'Data/Hora de Inclusao do Registro de PESSOA',
PEFI_DH_ALTREGIS: 'Data/Hora de Alteracao do Registro de PESSOA',
PEFI_NU_SCOREHSPN: 'SCORE HSPN CADASTRADO NO SERASA',
PEFI_VL_RENDAPRESUM: 'VALOR DE RENDA DA PESSOA NO SERASA',
PEFI_NU_SCOREHGC0: 'Número de Score do Modelo HGC0 cadastrado no Serasa',
PEFI_NU_SCOREHGC1: 'Número de Score do Modelo HGC1 cadastrado no Serasa',
PEFI_NU_PERFILHGC0: 'Número do Perfil HGC0 cadastrado no Serasa - primeiro digito do Score HGC0',
PEFI_NU_MODCUSTOMHGC0: 'Número do Modelo Customizado Digital HGC0 cadastrado no Serasa - 3 ultimos digitos do Score HGC0',

},

    data_governance: [
        {name: "schedule_name", value: "Not applicable"},
        {name: "data_classification", value: "Confidential"},
        {name: "partitioned_table", value: "false"},
        {name: "clustered_table", value: "false"},
        {name: "update_frequency", value: "Daily"},
        {name: "load_type", value: "Incremental"},
        {name: "data_retention", value: "Not Applicable"}
    ],

    policy_tags: {
        PEFI_NU_CPF: 'pii',
        PEFI_NM_PESS: 'pii',
        PEFI_DT_NASC: 'pii',
        PEFI_VL_RENDAINFO: 'pii'
    },

}

csf_modules.transformation
    .create_new_object(params)
    .query(ctx => `

        WITH table_layout AS (

            SELECT
            array<struct<
                PESS_ID_PESS NUMERIC
                , PEFI_NU_CPF STRING
                , PEFI_NM_PESS STRING
                , PEFI_DT_NASC DATETIME
                , PEFI_VL_RENDAINFO BIGNUMERIC
                , PEFI_VL_RENDACOMPROV BIGNUMERIC
                , PEFI_CD_ATIVDEPROFIS NUMERIC
                , PEFI_FG_FUNC STRING
                , PEFI_CD_ESTCIVIL STRING
                , PEFI_CD_CLASSIFSOC STRING
                , PEFI_CD_SEXO STRING
                , TPRO_CD_PROFIS NUMERIC
                , STOR_CD_SISTORI NUMERIC
                , PEFI_DH_INCLSISTORI DATETIME
                , PEFI_DH_INCLREGIS DATETIME
                , PEFI_DH_ALTREGIS DATETIME
                , PEFI_NU_SCOREHSPN NUMERIC
                , PEFI_VL_RENDAPRESUM BIGNUMERIC
                , PEFI_NU_SCOREHGC0 NUMERIC
                , PEFI_NU_SCOREHGC1 NUMERIC
                , PEFI_NU_PERFILHGC0 NUMERIC
                , PEFI_NU_MODCUSTOMHGC0 NUMERIC

                >>

            [] AS array_columns

        )

        SELECT
            PESS_ID_PESS
            , PEFI_NU_CPF
            , PEFI_NM_PESS
            , PEFI_DT_NASC
            , PEFI_VL_RENDAINFO
            , PEFI_VL_RENDACOMPROV
            , PEFI_CD_ATIVDEPROFIS
            , PEFI_FG_FUNC
            , PEFI_CD_ESTCIVIL
            , PEFI_CD_CLASSIFSOC
            , PEFI_CD_SEXO
            , TPRO_CD_PROFIS
            , STOR_CD_SISTORI
            , PEFI_DH_INCLSISTORI
            , PEFI_DH_INCLREGIS
            , PEFI_DH_ALTREGIS
            , PEFI_NU_SCOREHSPN
            , PEFI_VL_RENDAPRESUM
            , PEFI_NU_SCOREHGC0
            , PEFI_NU_SCOREHGC1
            , PEFI_NU_PERFILHGC0
            , PEFI_NU_MODCUSTOMHGC0

        FROM
            table_layout,
        UNNEST
            (table_layout.array_columns)

    `)
