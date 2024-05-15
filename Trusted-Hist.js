let params = {

    type: "incremental",
    dataset: "trusted",
    object_name: 'tsys_servicefees_hist',
    object_description:"Tabela de dados transacionais do sistema TSYS que representa as Taxas de Serviço",
    partition_by: {column: 'ingestion_timestamp', type: 'timestamp', granularity: 'day'},
    unique_key: ["serno", "ingestion_ref_date"],
    cluster_by: ["ingestion_ref_date"],
    pec: 2,
    pec_obs: 'PIT 0002 - Tsys',
    custom_tags:['PIT0002'],
    developer: ["matheus_felippe_silva_ext@carrefour.com"],
    pre_ops_declaration: `DECLARE P_MAX_TIMESTAMP TIMESTAMP DEFAULT (SELECT COALESCE(MAX(ingestion_timestamp), TIMESTAMP('1900-01-01')) FROM \`${dataform.projectConfig.defaultDatabase}.trusted.tsys_servicefees_hist\`);`,
    columns: ["*"] ,

 policy_tags: {

      },
    data_governance: [
          {name: "schedule_name", value: "tsys_servicefees"}
        , {name: "data_classification", value: "Internal"}
        , {name: "partitioned_table", value: "true"}
        , {name: "clustered_table", value: "true"}
        , {name: "update_frequency", value: "Daily"}
        , {name: "load_type", value: "Incremental"}
        , {name: "data_retention", value: "Not Applicable"}
    ],

    columns_description : {
          type: 'O tipo de registro: * I: Registro recém inserido, válido apenas quando incremental. *  U: Registro atualizado, válido apenas quando incremental. * D: Registro excluído, válido apenas quando incremental. Para registros excluídos, apenas o número de série da entidade será devolvido. * F: Solicitação de exportação completa'
        , institution_id: 'ID da instituição para implementar várias instituições'
        , serno: 'Número de série permanente exclusivo '
        , entity_indicator: 'Flag que indica a que entidade se aplica a taxa: * C: Cartões. * M: Localização do adquirente. * T: Terminal do adquirente. * S: Fatura do emissor. * A: Conta do emissor. * F: Conta do adquirente. * E: Fatura do adquirente. * 1: Transação do adquirente. * 0: Transação do emissor'
        , profile_serno: 'Número de Série do Perfil da Taxa de Serviço '
        , service_type: 'Tipo de serviço para o qual se define esta taxa'
        , service_reason: 'Motivo ou ação do serviço'
        , transaction_type: 'Texto para o código de motivo da transação da taxa'
        , description: 'Descrição'
        , sign: 'Sinal de postagem. Determina como a taxa é postada: * S: Mesmo sinal do saldo da conta. * O: Sinal oposto do saldo da conta. * C: Sempre crédito. * D: Sempre débito'
        , flat_amount: 'Montante fixo de comissão por transação'
        , currency: 'Moeda da taxa'
        , percent_amount: 'Porcentagem de comissão por transação'
        , percent_of_what: 'Montante pelo qual os juros são calculados'
        , combination: 'Combinação fixa/percentual. Determina como as taxas são calculadas, se houver um montante fixo e uma porcentagem: * -1: Use o montante que for menor. * 0: Adicione os dois montantes para calcular a taxa. * 1: Use o montante que for maior'
        , percent_applies_to: 'Determina qual entidade é usada para calcular a porcentagem. Valores válidos: * C: Cartão. * B: Grupo de Cobrança. * D: Domínio. * U: Cliente'
        , logversion: 'Número de vezes que se modifica um detalhe'
        , feebillingoption: 'Opção de cobrança'
        , periodicfeq: 'Frequência'
        , capitalize: 'Indica se os juros são capitalizados'
        , min_amount: 'Montante mínimo da taxa de serviço'
        , max_amount: 'Montante máximo da taxa de serviço'
        , suppressfee: 'Determina se deve impedir que essas taxas sejam cobradas na conta para: * 1: Suprimir no saldo zero. * 2: Suprimir no saldo credor. * 3: Ambos. * 4: Suprimir se o saldo e o montante do volume de serviço forem cobertos pelo pagamento do ciclo atual. * 0: Sem supressão'
        , instalmentplanshortcode: "Se a taxa for postada como parcela (ou seja, o valor do campo taxa da parcela for '1' ou '2'), o plano de parcelamento com este código curto será cobrado"
        , instalfee: 'Determina se as taxas devem ser postadas em parcelas. Valores: * 0: Nunca. * 1: Opcional. * 2: Sempre'
        , excludecurrentactivitybalance: 'Determina se a atividade atual deve ser excluída do montante do volume: * 0/Vazio - Incluir. * 1 - Excluir'
        , excludebilledbalance: 'Determina se os saldos consolidados devem ser excluídos do montante do volume: * 0/Vazio - Incluir. * 1 - Excluir'
        , usebalanceamountoption: 'Determina a opção de montante do saldo: * 0/Vazio - Saldo Pendente. * 1 - Saldo na Data de Vencimento. * 2 - Saldo Original'
        , curbalminusprevcyclebal: 'Determina se o montante do volume é a diferença entre o saldo do ciclo atual e o saldo de fechamento do ciclo anterior. * 0/Vazio - Incluir. * 1 - Excluir'
        , taxflag: 'Flag que indica se a taxa é um imposto'
        , countdaysfrom: 'Conte os dias a partir de: * Q - Data de vencimento da normalização'
        , countdaysto: 'Conte os dias para: * S - Data da fatura. * D - Data de vencimento'
        , serviceschedulerplanshortcode: 'O código curto para o plano do agendador de serviço'
        , trxncurrency: 'Moeda em que este serviço será postado'
        , refundable: 'Determina se a taxa de serviço pode ser reembolsada ou não. Valores válidos: * 0 = não é reembolsável. * 1 = é reembolsável'
        , tieredflag: 'Determina se a taxa de serviço é uma taxa escalonada ou não. Valores válidos: * 0/vazio: taxa normal, não escalonada. * 1: taxa escalonada'
        , percentratedays: 'Número de dias em um ano para fins de cálculo do percentual da taxa como taxa diária. Geralmente 360 ou 365'
        , addinstoutsrevolvingamount: 'Suporta a adição de montante rotativo pendente de parcela ao ADB ao calcular taxas'
        , instalmenttypes: 'Contribuição de diferentes tipos de parcela para base de taxas. Os valores atualmente suportados são: * 30: tipo de parcela INTRA. * 71: tipo de parcela EXTRA'
        , min_max_amount_currency: "Se esta coluna for definida, o montante mínimo ou máximo será calculado com esta moeda. Se não for definido, o cálculo usará a moeda na coluna 'Currency'"
    }

}

csf_modules.transformation.create_new_object(params).query (ctx => `

-- LISTA TEMPORÁRIA DAS CHAVES CARREGADAS NA CAMADA RAW

WITH LISTA_DE_CHAVES_CARREGADAS AS (

    SELECT DISTINCT serno
    FROM ${ctx.ref("raw", "tsys_servicefees_clean")}
    ${ctx.when(ctx.incremental(), `WHERE ingestion_timestamp > P_MAX_TIMESTAMP`)}

)

-- RETORNO COM TODAS AS LINHAS A SEREM PROCESSADAS (INCLUINDO NOVOS REGISTROS E REGISTROS COM VIGÊNCIA A SER FECHADA)

SELECT
    PARSE_DATE('%Y%m%d', CAST(ingestion_ref_date AS STRING)) AS dat_inicio_vigencia
  , LEAD(PARSE_DATE('%Y%m%d', CAST(ingestion_ref_date AS STRING))) OVER(PARTITION BY serno ORDER BY ingestion_ref_date, ingestion_timestamp) AS dat_fim_vigencia
  , TB.*
FROM
  ${ctx.ref("raw", "tsys_servicefees_clean")} AS TB
WHERE
  EXISTS (SELECT 1 FROM LISTA_DE_CHAVES_CARREGADAS AS LISTA WHERE LISTA.serno = TB.serno)

`);
