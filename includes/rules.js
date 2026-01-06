// Função para categorizar o valor da venda
function classify_transaction(column_name) {
  return `CASE
            WHEN ${column_name} > 1000 THEN 'ALTO_VALOR'
            WHEN ${column_name} > 100 THEN 'MEDIO_VALOR'
            ELSE 'BAIXO_VALOR'
          END`;
}

// Exporta a função para ser usada nos arquivos .sqlx
module.exports = {
  classify_transaction,
};
