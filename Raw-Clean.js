let params = {

    type: 'incremental',
    dataset: 'raw',
    object_name: 'tsys_servicefees_clean',
    object_description: 'Tabela de dados transacionais do sistema TSYS que representa as Taxas de Serviço',
    partition_by: {column: 'ingestion_timestamp', type: 'timestamp', granularity: 'day'},
    cluster_by: ["ingestion_ref_date"],
    unique_key: ["serno", "ingestion_ref_date"],
    pec: 2,
    pec_obs: 'PIT 0002 - TSYS',
    custom_tags:['PIT0002'],
    developer: ["matheus_felippe_silva_ext@carrefour.com"],
    pre_ops_declaration: `DECLARE P_MAX_TIMESTAMP TIMESTAMP DEFAULT (SELECT COALESCE(MAX(ingestion_timestamp), TIMESTAMP('1900-01-01')) FROM \`${dataform.projectConfig.defaultDatabase}.raw.tsys_servicefees_clean\`);`,
    columns: ["*"],

    data_governance: [
          {name: "schedule_name", value: "tsys_servicefees"}
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
    FROM ${ctx.ref("raw", "tsys_servicefees")}
    ${ctx.when(ctx.incremental(), `WHERE ingestion_timestamp > P_MAX_TIMESTAMP`)}

)

-- RETORNO COM TRATAMENTO DE LINHAS COM COLUNA TYPE D

SELECT * FROM (
    SELECT
          type
        , IF(UPPER(type) = 'D' AND institution_id IS NULL, LEAD(institution_id) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), institution_id) AS institution_id
        , serno
        , IF(UPPER(type) = 'D' AND entity_indicator IS NULL, LEAD(entity_indicator) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), entity_indicator) AS entity_indicator
        , IF(UPPER(type) = 'D' AND profile_serno IS NULL, LEAD(profile_serno) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), profile_serno) AS profile_serno
        , IF(UPPER(type) = 'D' AND service_type IS NULL, LEAD(service_type) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), service_type) AS service_type
        , IF(UPPER(type) = 'D' AND service_reason IS NULL, LEAD(service_reason) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), service_reason) AS service_reason
        , IF(UPPER(type) = 'D' AND transaction_type IS NULL, LEAD(transaction_type) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), transaction_type) AS transaction_type
        , IF(UPPER(type) = 'D' AND description IS NULL, LEAD(description) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), description) AS description
        , IF(UPPER(type) = 'D' AND sign IS NULL, LEAD(sign) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), sign) AS sign
        , IF(UPPER(type) = 'D' AND flat_amount IS NULL, LEAD(flat_amount) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), flat_amount) AS flat_amount
        , IF(UPPER(type) = 'D' AND currency IS NULL, LEAD(currency) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), currency) AS currency
        , IF(UPPER(type) = 'D' AND percent_amount IS NULL, LEAD(percent_amount) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), percent_amount) AS percent_amount
        , IF(UPPER(type) = 'D' AND percent_of_what IS NULL, LEAD(percent_of_what) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), percent_of_what) AS percent_of_what
        , IF(UPPER(type) = 'D' AND combination IS NULL, LEAD(combination) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), combination) AS combination
        , IF(UPPER(type) = 'D' AND percent_applies_to IS NULL, LEAD(percent_applies_to) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), percent_applies_to) AS percent_applies_to
        , IF(UPPER(type) = 'D' AND logversion IS NULL, LEAD(logversion) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), logversion) AS logversion
        , IF(UPPER(type) = 'D' AND feebillingoption IS NULL, LEAD(feebillingoption) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), feebillingoption) AS feebillingoption
        , IF(UPPER(type) = 'D' AND periodicfeq IS NULL, LEAD(periodicfeq) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), periodicfeq) AS periodicfeq
        , IF(UPPER(type) = 'D' AND capitalize IS NULL, LEAD(capitalize) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), capitalize) AS capitalize
        , IF(UPPER(type) = 'D' AND min_amount IS NULL, LEAD(min_amount) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), min_amount) AS min_amount
        , IF(UPPER(type) = 'D' AND max_amount IS NULL, LEAD(max_amount) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), max_amount) AS max_amount
        , IF(UPPER(type) = 'D' AND suppressfee IS NULL, LEAD(suppressfee) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), suppressfee) AS suppressfee
        , IF(UPPER(type) = 'D' AND instalmentplanshortcode IS NULL, LEAD(instalmentplanshortcode) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), instalmentplanshortcode) AS instalmentplanshortcode
        , IF(UPPER(type) = 'D' AND instalfee IS NULL, LEAD(instalfee) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), instalfee) AS instalfee
        , IF(UPPER(type) = 'D' AND excludecurrentactivitybalance IS NULL, LEAD(excludecurrentactivitybalance) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), excludecurrentactivitybalance) AS excludecurrentactivitybalance
        , IF(UPPER(type) = 'D' AND excludebilledbalance IS NULL, LEAD(excludebilledbalance) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), excludebilledbalance) AS excludebilledbalance
        , IF(UPPER(type) = 'D' AND usebalanceamountoption IS NULL, LEAD(usebalanceamountoption) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), usebalanceamountoption) AS usebalanceamountoption
        , IF(UPPER(type) = 'D' AND curbalminusprevcyclebal IS NULL, LEAD(curbalminusprevcyclebal) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), curbalminusprevcyclebal) AS curbalminusprevcyclebal
        , IF(UPPER(type) = 'D' AND taxflag IS NULL, LEAD(taxflag) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), taxflag) AS taxflag
        , IF(UPPER(type) = 'D' AND countdaysfrom IS NULL, LEAD(countdaysfrom) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), countdaysfrom) AS countdaysfrom
        , IF(UPPER(type) = 'D' AND countdaysto IS NULL, LEAD(countdaysto) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), countdaysto) AS countdaysto
        , IF(UPPER(type) = 'D' AND serviceschedulerplanshortcode IS NULL, LEAD(serviceschedulerplanshortcode) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), serviceschedulerplanshortcode) AS serviceschedulerplanshortcode
        , IF(UPPER(type) = 'D' AND trxncurrency IS NULL, LEAD(trxncurrency) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), trxncurrency) AS trxncurrency
        , IF(UPPER(type) = 'D' AND refundable IS NULL, LEAD(refundable) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), refundable) AS refundable
        , IF(UPPER(type) = 'D' AND tieredflag IS NULL, LEAD(tieredflag) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), tieredflag) AS tieredflag
        , IF(UPPER(type) = 'D' AND percentratedays IS NULL, LEAD(percentratedays) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), percentratedays) AS percentratedays
        , IF(UPPER(type) = 'D' AND addinstoutsrevolvingamount IS NULL, LEAD(addinstoutsrevolvingamount) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), addinstoutsrevolvingamount) AS addinstoutsrevolvingamount
        , IF(UPPER(type) = 'D' AND instalmenttypes IS NULL, LEAD(instalmenttypes) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), instalmenttypes) AS instalmenttypes
        , IF(UPPER(type) = 'D' AND min_max_amount_currency IS NULL, LEAD(min_max_amount_currency) OVER(PARTITION BY serno ORDER BY ingestion_ref_date DESC), min_max_amount_currency) AS min_max_amount_currency
        , ingestion_timestamp
        , ingestion_source_file
        , ingestion_proc_datetime
        , ingestion_ref_date
    FROM
        ${ctx.ref("raw", "tsys_servicefees")} AS TB
    WHERE
        EXISTS (SELECT 1 FROM LISTA_DE_CHAVES_CARREGADAS AS LISTA WHERE LISTA.serno = TB.serno)
) ${ctx.when(ctx.incremental(), `WHERE ingestion_timestamp > P_MAX_TIMESTAMP`)}

`);
