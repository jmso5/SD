# SD projeto meta 1

## Sobre o projeto
O googol é um motor de busca distribuído, que utiliza o protocolo gRPC para a comunicação entre componentes. Simulando um processo de indexação de URLs (crawling), que armazena um índice invertido e disponibilização de serviços de pesquisa, estatisticas e backlinks.

---

## Arquitetura
Este projeto é composto por três componentes principais:
    - Gateway é o ponto de entrada para os clientes: encaminha pedidos de pesquisa e de backlinks; gere a lista de URLs a visitar, distribuindo a carga pelos barrels; agrega estatísticas de todos os componentes.
    - Downloader é o crawler do sistema: recebe URLs da fila e descarrega a pagina; extrai título, texto e links da página; extrai as palavras e informação da página para os barrels; mantém um dicionário local de backlinks e responde ao gateway quando solicitado.
    - Barrel armazenam e indexam: mantém o índice invertido do motor de busca; armazena a fila de URLs localmente, replicando as adições para outras réplicas; gere os pedidos de pesquisa.

---

## Como executar o projeto
1. **Instalação**:
    - É necessário instalar as bibliotecas do python necessárias, para tal, escrever 'pip install grpcio grpcio-tools requests beautifulsoup4 fastapi' no terminal ou powershell.
2. **Configuração**:
    - O ficheiro 'config.json' já define todos os endereços necessários para o sistema.
3. **Instalação dos ficheiros index_pb2**
    - Para instalar os ficheiros index_pb2_grpc.py e index_pb2.py basta executar o seguinte comando no terminal:
    ```bash
      python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. index.proto`
    ```
4. **Execução**:
    1- Inicializar os barrels - precisa de 3 terminais:
   ```bash
        terminal 1 -> python barrel.py barrel_1;
        terminal 2 -> python barrel.py barrel_2;
   ```
    2- Inicializar o gateway:
        terminal -> python gateway.py;
    3- Inicializar os downloaders - precisa de 3 terminais:
   ```bash
        terminal 1 -> python downloader.py downloader_1;
        terminal 2 -> python downloader.py downloader_2;
        terminal 3 -> python downloader.py downloader_3;
   ```
    4- Inicializar o cliente:
   ```bash
        terminal -> python client.py;
   ```

---

## Detalhes da implementação
1. **Estrutura de dados**:
    - O barrel utiliza o dicionário indice_invertido para armazenamento dos links, no qual os resultados são ordenados pela sua relevância (links_count).
    - A fila de URLs implementada com a classe queue.Queue no barrel e persistida em disco através do uso d pickle para a tolerância de falhas.
2. **Comunicação gRPC**:
    - Os serviços são definidos pelo o index.proto:
        -> StorageBarrel: fornece métodos como addToIndex, searchWord, putNew, takeNext, getIndexSize.
        -> Gateway: fornece métodos como putNew, takeNext, searchWord, getBackLinks, getStatistics.
        -> Downloader: fornece o método getBackLinks.
3. **Mecanismos chave**:
    - Tolerância a falhas/distribuição: o gateway usa a seleção aleatória para distribuir a carga das operações de fila pelos barrels ativos.
    - Replicação: o barrel replica as chamadas putNew para as réplicas, garantindo que a fila de URLs persista em múltiplos nós.
    - Caching: o gateway implementa uma cache local para os resultados de pesquisa, otimizando o tempo de resposta para pesquisas repetidas.

---

## Testes executados:
1. **Dois computadores**:
    - Foram inicalizados um Barrel, um Downloader e um Cliente num dos computadores e um Barrel, um Downloader e a Gateway no outro.
2. **Executar com um só Barrel ou Downloader**
    - Foram incializados apenas um Barrel e um Downloader para verificar se o sistema funcionaria sem serem inicializados todos os ficheiros presentes no config file. Foram a meio inicializados os ficheiros restantes e desligados novamente.


---


# SD projeto meta 2

## Instruções para configuração e execução
1. **Configuração:**
    - Certificar que o ficheiro `config.json` está corretamente configurado com os IPs e portas de todos os componentes:
        * Endereços dos Barrels.
        * Endereço do Gateway.
        * Endereços dos Downloaders.
        * Chave API do Gemini.

2. **Execução:**
    - Certifique-se que tem todas as bibliotecas necessárias instaladas, caso use o "pip" execute este comando no seu terminal para evitar erros de bibliotecas:
        ```bash
            pip install grpcio grpcio-tools requests beautifulsoup4 fastapi uvicorn jinja2 python-multipart websockets google-genai protobuf httpx
        ```  

    - Certifique-se que está dentro da pasta `backend`:
        1 - Inicializar os barrels:
      ```bash
            terminal 1 -> python barrel.py barrel_1;
            terminal 2 -> python barrel.py barrel_2;
      ```
        2- Inicializar o gateway:
      ```bash
            terminal -> python gateway.py
      ```
        3- Inicializar os downloaders:
      ```bash
            terminal 1 -> python downloader.py downloader_1;
            terminal 2 -> python downloader.py downloader_2;
      ```
    - Certifique-se que está dentro da pasta `web`:
        1 - Inicializar routes:
      ```bash
            terminal -> uvicorn routes:app --reload;
      ```
--- 

## Tabela de Testes
| **Teste**                                   | **Descrição**                                                                 | **Resultado** |
|---------------------------------------------|-------------------------------------------------------------------------------|---------------|
| Indexação manual de um URL                  | URL é enviado ao gateway, colocado na fila e depois descarregado e indexado   | Pass          |
| Pesquisa de uma palavra                     | O gateway consulta todos os barrels e devolve os resultados                   | Pass          |
| Pesquisa de múltiplas palavras              | Só as páginas com todas as palavras aparecem                                  | Pass          |
| Ordenação por número de backlinks           | Os resultados de pesquisa aparecem ordenados por ligações recebidas           | Pass          |
| Paginação 10 em 10                          | Os botões next/prev mudam a página, aparecendo em cada página 10 URLs         | Pass          |
| Backlinks                                   | Devolve todas as páginas associadas a uma página                              | Pass          |
| Estatísticas                                | Frontend atualiza automaticamente valores quando estas mudam                  | Pass          |
| Top 10 pesquisas mais comuns                | Pesquisas mais comuns atualizam o ranking em tempo real                       | Pass          |
| Lista de barrels ativos e tamanhos do índice| WebSocket mostra lista atualizada                                             | Pass          |
| Tempo de resposta do gateway                | Cada pesquisa mostra o tempo de execução                                      | Pass          |
| API Hacker News                             | URLs relevantes são filtrados e enviados para o gateway                       | Pass          |
| API Gemini                                  | O prompt é enviado e recebe-se o texto contextualizado                        | Pass          |

---

## Arquitetura de Software
1. **Interface web**
    - O módulo `routes.py` atua como o controller da aplicação, tendo as seguintes funcionalidades:
        * Pesquisa `search_word`-> faz chamadas gRPC síncronas ao gateway para obter resultados de pesquisa e aplica paginação.
        * Indexação manual `index_url` -> envia URL ao gateway para ser adicionado à fila.
        * Estatísticas `staws/stats` -> utiliza WebSockets para manter uma conexão persistente. O endpoint faz polling ao gateway e envia a atualização JSON ao frontend apenas se as estatísticas mudaram, otimizando o tráfego.
        * Análise IA `geminiAPI` -> Interage com o gemini-2.5-flash, contruindo um prompt detalhado com os termos de pesquisa e snippets para gerar um resumo contextualizado.
    - A interface é responsável por receber pesquisas e pedidos do utilizador, traduzir essas ações em chamadas RPC para o gateway, renderizar páginas HTML Jinja2, fornecer endpoints REST adicionais e manter Websockets ativos para estatísticas em tempo real. Sendo, o ficheiro principal o `routes.py`, que centraliza: rotas HTML; rota JSON, WebSockets, chamadas externas via REST e integração com a API Gemini.

2. **Servidor RPC**
    - *Gateway:* Recebe pedidos de pesquisa da interface web, distribui pelos barrels e coordena a fila de indexação.
    - *Barrels:* Indexão o conteúdo das páginas web e respondem a consultas de pesquisa do gateway.
    - *Downloaders:* Responsáveis por obter o conteúdo de URLs e enviá-lo para indexação no gateway.
    - *Interface web:* A aplicação que permite realizar pesquisas, ver resultados pesquisados, obter backlinks, indexar URLs manualmente e ver estatísticas em tempo real via WebSocket.

---

## Integração de WebSockets
O websocket implementado em `ws/stats`faz polling ao gateway através de *client_instance.statistics()*
1. **WebSocket**
    - Mantém ligação persistente com o browser
    - Compara estatísticas novas com as anteriores, enviando apenas se forem diferentes, o que reduz o tráfego
    - Atualiza em tempo real no frontend o número de URLs na fila, o tamanho dos índices dos barrels, as top 10 pesquisas mais comuns e os tempos de resposta.

2. **Integração direta entre**
    - FasAPI;
    - Gateway; 
    - Frontend; 

---

## Integração de REST WebServices
1. **HackerNews API**
    - Obtem lista top stories;
    - Filtrar histórias que tenham os termos pesquisados;
    - Enviar URLs para indexação;

2. **Gemini API**
    - Resumos contextualizados;
    - Análise de padrões nos snippets;
    - Textos informativos;


