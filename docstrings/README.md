# SD projeto meta 2

## Instruções para configuração e execução
1. **Configuração:**
    - Certificar que o ficheiro `config.json` está corretamente configurado com os IPs e portas de todos os componentes:
        * Endereços dos Barrels.
        * Endereço do Gateway.
        * Endereços dos Downloaders.
        * Chave API do Gemini

2. **Execução:**
    - Certifique-se que está dentro da pasta `backend`
        1 - Inicializar os barrels:
            terminal 1 -> python barrel.py barrel_1
            terminal 2 -> python barrel.py barrel_2
        2- Inicializar o gateway:
            terminal -> python gateway.py
        3- Inicializar os downloaders:
            terminal 1 -> python downloader.py downloader_1
            terminal 2 -> python downloader.py downloader_2
    - Certifique-se que está dentro da pasta `web`
        1 - Inicializar routes:
            terminal -> uvicorn routes:app --reload

## Testes de Software
Teste | Descrição | Pass Fail
**Indexação manual de um URL** | URL é enviado ao gateway, colocado na fila e depois descarregado e indexado | Pass
**Pesquisa de uma palavra** | O gateway consulta todos os barrels e devolve os resultados | Pass
**Pesquisa de múltiplas palavras** | Só as páginas com todas as palavras aparecem | Pass
**Ordenação por número de backlinks** | Os resultados de pesquisa aparecem ordenados por ligações recebidas | Pass
**Paginação 10 em 10** | Os botões next/prev mudam a página, aparecendo em cada página 10 URLs | Pass
**Backlinks** | Devolve todas as páginas associadas a uma página | Pass
**Estatísticas** | Frontend atualiza automaticamente valores quando estas mudam | Pass
**Top 10 pesquisas mais comuns** | Pesquisas mais comuns atualizam o ranking em tempo real | Pass
**Lista de barrels ativos e tamanhos do índice** | WebSocket mostra lista atualizada | Pass
**Tempo de resposta do gateway** | Cada pesquisa mostra o tempo de execução | Pass
**API Hacker news** | URLs relevantes são filtrados e enviados para o gateway | Pass
**API Gemini** | O prompt é enviado e recebe-se o texto contextualizado | Pass
**Health Check** | Devolve {status: online} | Pass
**Página offline** | Aparece Offline.html quando o servidor falha | Pass

## Arquitetura de Software
1. **Interface web**
    - O módulo `routes.py` atua como o controller da aplicação, tendo as seguintes funcionalidades:
        * Pesquisa `search_word`-> faz chamadas gRPC síncronas ao gateway para obter resultados de pesquisa e aplica paginação.
        * Indexação manual `index_url` -> envia URL ao gateway para ser adicionado à fila.
        * Estatísticas `staws/stats` -> utiliza WebSockets para manter uma conexão persistente. O endpoint faz polling ao gateway e envia a atualização JSON ao frontend apenas se as estatísticas mudaram, otimizando o tráfego.
        * Análise IA `geminiAPI` -> Interage com o gemini-2.5-flash, contruindo um prompt detalhado com os termos de pesquisa e snippets para gerar um resumo contextualizado
    - A interface é responsável por receber pesquisas e pedidos do utilizador, traduzir essas ações em chamadas RPC para o gateway, renderizar páginas HTML Jinja2, fornecer endpoints REST adicionais e manter Websockets ativos para estatísticas em tempo real. Sendo, o ficheiro principal o `routes.py`, que centraliza: rotas HTML; rota JSON, WebSockets, chamadas externas via REST e integração com a API Gemini

2. **Servidor RPC**
    - *Gateway:* Recebe pedidos de pesquisa da interface web, distribui pelos barrels e coordena a fila de indexação.
    - *Barrels:* Indexão o conteúdo das páginas web e respondem a consultas de pesquisa do gateway.
    - *Downloaders:* Responsáveis por obter o conteúdo de URLs e enviá-lo para indexação no gateway.
    - *Interface web:* A aplicação que permite realizar pesquisas, ver resultados pesquisados, obter backlinks, indexar URLs manualmente e ver estatísticas em tempo real via WebSocket.

## Integração de WebSockets
O websocket implementado em `ws/stats`faz polling ao gateway através de *client_instance.statistics()*
1. **WebSocket**
    - mantém ligação persistente com o browser
    - compara estatísticas novas com as anteriores, enviando apenas se forem diferentes, o que reduz o tráfego
    - atualiza em tempo real no frontend o número de URLs na fila, o tamanho dos índices dos barrels, as top 10 pesquisas mais comuns e os tempos de resposta

2. **Integração direta entre**
    - FasAPI
    - Gateway 
    - Frontend 

## Integração de REST WebServices
1. **HackerNews API**
    - obtem lista top stories
    - filtrar histórias que tenham os termos pesquisados
    - enviar URLs para indexação

2. **Gemini API**
    - resumos contextualizados
    - análise de padrões nos snippets
    - textos informativos