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

## Arquitetura
1. **Interface web**
    - O módulo `routes.py` atua como o controller da aplicação, tendo as seguintes funcionalidades:
        * Pesquisa `search_word`-> faz chamadas gRPC síncronas ao gateway para obter resultados de pesquisa e aplica paginação.
        * Indexação manual `index_url` -> envia URL ao gateway para ser adicionado à fila.
        * Estatísticas `staws/stats` -> utiliza WebSockets para manter uma conexão persistente. O endpoint faz polling ao gateway e envia a atualização JSON ao frontend apenas se as estatísticas mudaram, otimizando o tráfego.
        * Análise IS `geminiAPI` -> Interage com o gemini-2.5-flash, contruindo um prompt detalhado com os termos de pesquisa e snippets para gerar um resumo contextualizado

## Componentes chave
1. **Gateway:** Recebe pedidos de pesquisa da interface web, distribui pelos barrels e coordena a fila de indexação.
2. **Barrels:** Indexão o conteúdo das páginas web e respondem a consultas de pesquisa do gateway.
3. **Downloaders:** Responsáveis por obter o conteúdo de URLs e enviá-lo para indexação no gateway.
4. **Interface web:** A aplicação que permite realizar pesquisas, ver resultados pesquisados, obter backlinks, indexar URLs manualmente e ver estatísticas em tempo real via WebSocket.

## Testes executados no frontend
1. **Adicionar URL manualmente:**
    - adicionamos manualmente um URL
2. **Pesquisa de palavra(s):**
    - pesquisámos uma ou mais palavras para ver os resultados e carregando no botão 'next' para ver mais do que os 10 resultados por paginação
3. **Estatísticas em tempo real:**
    - quando adicionávamos um URL, ou pesquisávamos uma palavra observávamos a alteração das estatísticas