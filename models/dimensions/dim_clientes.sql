{{ config(materialized='table') }}
SELECT
    id_clientes AS cliente_id,
    cliente AS nome_cliente,
    endereco,
    id_concessionarias AS concessionaria_id,
    data_inclusao,
    data_atualizacao
FROM {{ ref('stg_clientes') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id_clientes
    ORDER BY data_atualizacao DESC
) = 1
