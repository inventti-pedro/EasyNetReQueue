# EasyNetReQueue

Command Line Interface para facilitar a movimentação de mensagens da fila de erros `EasyNetQ` para outras filas.

### Build
`go build requeue.go`

### Parâmetros

Similar a ajuda obtida pelo `requeue --help`

- **cs**: String de conexão no formato protocolo `amqp`
- **cid**: ID de correlação (`IdCorrelacao`) da mensagem desejada
- **queue**: Fila destino
- **rm**: Remover da fila de erros

### Ajuda
Executar `requeue --help`
