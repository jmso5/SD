# SD projeto meta 1


## Sobre o projeto
O googol é um motor de busca distribuído, que utiliza o protocolo gRPC para a comunicação entre componentes. Simulando um processo de indexação de URLs (crawling), que armazena um índice invertido e disponibilização de serviços de pesquisa, estatisticas e backlinks.


## Arquitetura
Este projeto é composto por três componentes principais:
    - Gateway é o ponto de entrada para os clientes: encaminha pedidos de pesquisa e de backlinks; gere a lista de URLs a visitar, distribuindo a carga pelos barrels; agrega estatísticas de todos os componentes
    - Downloader é o crawler do sistema: recebe URLs da fila e descarrega a pagina; extrai título, texto e links da página; extrai as palavras e informação da página para os barrels; mantém um dicionário local de backlinks e responde ao gateway quando solicitado
    - Barrel armazenam e indexam: mantém o índice invertido do motor de busca; armazena a fila de URLs localmente, replicando as adições para outras réplicas; gere os pedidos de pesquisa


## Como executar o projeto
1. **Instalação**:
    - É necessário instalar as bibliotecas do python necessárias, para tal, escrever 'pip install grpcio grpcio-tools requests beautifulsoup4' no terminal ou powershell
2. **Configuração**:
    - O ficheiro 'config.json' já define todos os endereços necessários para o sistema.
3. **Instalação dos ficheiros index_pb2**
    - Para instalar os ficheiros index_pb2_grpc.py e index_pb2.py basta executar o seguinte comando no terminal:
      `python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. index.proto`
4. **Execução**:
    1- Inicializar os barrels - precisa de 3 terminais:
        terminal 1 -> python barrel.py barrel_1
        terminal 2 -> python barrel.py barrel_2
        terminal 3 -> python barrel.py barrel_3
    2- Inicializar o gateway:
        terminal -> python gateway.py
    3- Inicializar os downloaders - precisa de 3 terminais:
        terminal 1 -> python downloader.py downloader_1
        terminal 2 -> python downloader.py downloader_2
        terminal 3 -> python downloader.py downloader_3
    4- Inicializar o cliente:
        terminal -> python client.py


## Detalhes da implementação
1. **Estrutura de dados**:
    - O barrel utiliza o dicionário indice_invertido para armazenamento dos links, no qual os resultados são ordenados pela sua relevância (links_count)
    - A fila de URLs implementada com a classe queue.Queue no barrel e persistida em disco através do uso d pickle para a tolerância de falhas
2. **Comunicação gRPC**:
    - Os serviços são definidos pelo o index.proto:
        -> StorageBarrel: fornece métodos como addToIndex, searchWord, putNew, takeNext, getIndexSize
        -> Gateway: fornece métodos como putNew, takeNext, searchWord, getBackLinks, getStatistics 
        -> Downloader: fornece o método getBackLinks 
3. **Mecanismos chave**:
    - Tolerância a falhas/distribuição: o gateway usa a seleção aleatória para distribuir a carga das operações de fila pelos barrels ativos
    - Replicação: o barrel replica as chamadas putNew para as réplicas, garantindo que a fila de URLs persista em múltiplos nós
    - Caching: o gateway implementa uma cache local para os resultados de pesquisa, otimizando o tempo de resposta para pesquisas repetidas 