"""
Módulo Downloader

    - Componente responsável por fazer o download das páginas web executando em paralelo, 
    extraindo o conteúdo e os links através de parsing HTML, e enviando a informação para os Storage Barrels.
    - Estabelece comunicação gRPC com o Gateway para obter URLs a visitar e enviar novos URLs descobertos.

    Funcionalidades principais:
        - Download de páginas web.
        - Extração de título, texto e links.
        - Envio de dados para os Storage Barrels via gRPC.
        - Comunicação com o Gateway para gestão da fila de URLs.
        - Serviço gRPC para responder a pedidos de backlinks do Gateway.

    - Cada Downloader opera num loop contínuo, solicitando URLs à fila de URLs por visitar, esse pedido é feito
    através do Gateway (putNew) para os Barrels, que é onde está a fila de URLs por visitar.
    - Os links extraídos são enviados de volta para o Gateway (putNew) para serem adicionados à fila de URLs por visitar.
    - Após descarregar e processar uma página, o Downloader envia os dados extraídos para os Barrels.

    Failover:
        - O Downloader verifica periodicamente a disponibilidade dos Barrels.
        - Em caso de falha na comunicação com um Barrel, o Downloader tenta reconectar-se.
        - Se nenhum Barrel estiver disponível, o Downloader aguarda antes de tentar novamente.
        - É implementado um mecanismo de retries para chamadas gRPC falhadas, com um número máximo de tentativas.

    Protocolo de multicast:
        - Para garantir que todos os Barrels recebem os dados, o Downloader envia as informações extraídas para todos os Barrels disponíveis.
"""



from bs4 import BeautifulSoup # Biblioteca para parsear HTML e extrair texto/links
import requests # Para fazer o download das páginas
from urllib.parse import urljoin # Para transformar links relativos em absolutos
import grpc # Comunicação RPC com o Barrel
import index_pb2
import index_pb2_grpc
from google.protobuf import empty_pb2
import sys
import json
import time
from urllib.parse import urlparse, urlunparse
from concurrent import futures # Necessário para o servidor gRPC

# constantes globais 
MAX_RETRIES = 3 # número máximo de tentativas para chamadas RPC
MAX_WORKERS = 10 # para o ThreadPoolExecutor

class Downloader(index_pb2_grpc.DownloaderServicer):
    """
    - Implementa o serviço Downloader, responsável por extrair informação de páginas web.

    Atributos:
        - urls_visitadas: Conjunto de URLs já visitados para evitar repetições.
        - gateway_stub: Stub gRPC para comunicar com o Gateway.
        - barrel_stub: Lista de stubs gRPC para comunicar com os Storage Barrels.
    """

    def __init__(self, gateway_address):
        """
        - Inicializa o Downloader, estabelecendo conexões gRPC com os Barrels e o Gateway.
        """
        self.urls_visitadas = set()     # fila de URLs já visitados

        self.gateway_address = gateway_address
        self.gateway_stub = self.connect_gateway() # conexão gRPC com o Gateway


    
    def connect_gateway(self):
        """
        Método connect_gateway:
            - Estabelece a conexão gRPC com o Gateway, criando o canal gRPC e o stub necessário.
            - Devolve o stub para comunicação com o Gateway.
        """

        while True:
            try:
                channel = grpc.insecure_channel(self.gateway_address)
                stub = index_pb2_grpc.GatewayStub(channel) # stub para comunicar
                
                # Força uma chamada de teste (usando getStatistics)
                # Isto força o gRPC a tentar estabelecer a conexão TCP/HTTP2.
                stub.getStatistics(empty_pb2.Empty()) 
                
                print(f"[Gateway] Conexão em {self.gateway_address} estabelecida com sucesso.")
                return stub # Retorna o stub ativo
            
            except grpc.RpcError as e:
                # Se falhar a conexão ou der timeout
                print(f"[Gateway] Indisponível em {self.gateway_address} (Erro: {e.code().name}). A tentar novamente em 5 segundos...")
            except Exception as e:
                # Erro inesperado
                print(f"[Gateway] Erro desconhecido durante a conexão: {e}. A tentar novamente em 5 segundos...")
                
            time.sleep(5) # Espera antes de tentar novamente
    

    def validador_url(self, url):
        """
        Método validador_url:
            - Verifica se o URL é válido para processamento.
            - Ignora URLs com esquemas "javascript:", "mailto:", ou âncoras "#", aceitando apenas "http://" e "https://".
        """

        if not url:
            return False
        url_lower = url.lower()
        if url_lower.startswith("javascript:"):
            return False
        if url_lower.startswith("mailto:"):
            return False
        if url_lower.startswith("#"):
            return False
        if url_lower.startswith("http://") or url_lower.startswith("https://"):
            return True
        return False


    def processar_url(self, url):
        """
        Método processar_url:
            - Faz o download do conteúdo HTML de um URL.
            - Usa o referer base extraído do URL para evitar bloqueios de servidores que exigem referer.
            - É utilizado um User-Agent para evitar bloqueios por alguns servidores.
            - É utilizada a função requests.get que devolve o conteúdo HTML da página.
            - Retorna o conteúdo HTML se bem sucedido, ou None em caso de falha.
        """

        try:
            print(f"A descarregar: {url}")

            # Tenta extrair o "site" base para usar como Referer
            parsed_url = urlparse(url)
            referer_base = f"{parsed_url.scheme}://{parsed_url.netloc}/"

            # Define um User-Agent para evitar bloqueios por alguns servidores
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "pt-PT,pt;q=0.8,en-US;q=0.5,en;q=0.3",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "DNT": "1", # (Do Not Track - ironia, mas ajuda)
                "Referer": referer_base
            }

            resposta = requests.get(url,headers=headers, timeout=5) # faz pedido HTTP

            if resposta.status_code == 200 and 'text/html' in resposta.headers.get('Content-Type', '').lower(): # resposta.status_code == 200 -> verifica se o pedido foi bem sucedido
                self.urls_visitadas.add(url) # marca URL como visitado
                return resposta.text
            else:
                # CÓDIGO DE DEBUG MELHORADO
                c_type = resposta.headers.get('Content-Type', 'N/A')
                print(f"Ignorado: {url}")
                print(f"   -> Status: {resposta.status_code}")
                print(f"   -> Content-Type: {c_type}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"Falha ao descarregar {url}: {e}")
            return None

    def extrair_informacao(self, html, base_url):
        """
        Método extrair_informacao:
            - Extrai título, texto e links de uma página HTML.
            - Utiliza BeautifulSoup para parsear o HTML.
            - Converte links relativos em absolutos usando urljoin.
            - Retorna um dicionário com a informação extraída.
        """
        
        soup = BeautifulSoup(html, "html.parser")
        # extrai título da página
        if soup.title:
            titulo = soup.title.string
        else:
            titulo = "Sem título"

        texto = soup.get_text(separator=" ", strip=True) # extrai texto da página
        links = []

        for a in soup.find_all("a", href=True): # encontra todos os <a href="">
            link_absoluto = urljoin(base_url, a["href"])  # converte link relativo para absoluto
            links.append(link_absoluto)

        return {"url": base_url, "titulo": titulo, "texto": texto, "links": links}
    
    def getBacklinks(self, request, context):
        """
        Método getBacklinks:
            - Método gRPC para obter a lista de backlinks para um URL solicitado pelo cliente.
            - Utiliza os métodos internos para processar o URL e extrair os links.
            - Retorna uma resposta com a lista de backlinks encontrados.
        """

        url = request.url
        print(f"[Servidor RPC] Pedido de Backlinks para: {url}")

        try:
            html = self.processar_url(url)  # Garante que o URL foi processado

            if not html:
                print(f"[Servidor RPC] Não foi possível processar o URL: {url}")
                return index_pb2.BacklinksResponse(backlinks=[])
            
            info = self.extrair_informacao(html, url)
            backlinks = info["links"]  # Usa os links extraídos como backlinks

            print(f"[Servidor RPC] Encontrados {len(backlinks)} backlinks para {url}")
            return index_pb2.BacklinksResponse(backlinks=backlinks)
        except Exception as e:
            print(f"Erro ao obter backlinks para {url}: {e}")
            return index_pb2.BacklinksResponse(backlinks=[])

    
    def main_loop(self):

        """
        Método main_loop:
            - Pede continuamente URLs ao Gateway para processar (takeNext).
            - Se receber um URL, faz o download e extrai a informação.
            - Envia os dados extraídos para os Storage Barrels.
            - Adiciona novos links descobertos à fila através do Gateway (putNew).
        """

        while True:
            try:     
                resposta = self.gateway_stub.takeNext(empty_pb2.Empty()) # pede ao gateway o próximo URL
                url = resposta.url

                
                # se a fila estiver vazia, espera
                if not url:
                    print("[Fila] vazia, a esperar...")
                    time.sleep(3)
                    continue
                
                # evita repetir páginas
                if url in self.urls_visitadas: 
                    continue


                html = self.processar_url(url) # faz download da página
                if not html:
                    continue

                info = self.extrair_informacao(html, url)
                print(f"\nTÍTULO -> {info['titulo']}")
                print(f"URL -> {info['url']}")
                print(f"NÚMERO DE LINKS ENCONTRADOS -> {len(info['links'])}")
                print("-" * 60)


                # envia palavras para o Barrel
                palavras = info["texto"].lower().split()
                for palavra in palavras:
                    if palavra.isalpha(): # ignora números e símbolos
                            for attempt in range (MAX_RETRIES):
                                try:
                                    links = []
                                    for link in info['links']:
                                        links.append(link)

                                    # chama o método remoto na gateway
                                    self.gateway_stub.addToIndex(
                                        index_pb2.AddToIndexRequest(
                                            word=palavra,
                                            url=info["url"],
                                            title=info["titulo"],
                                            citacao=info["texto"][:200],  
                                            links_count=len(info['links'])
                                        )
                                    )
                                except grpc.RpcError as e:
                                    print(f"Erro RPC ao enviar palavra '{palavra}' para a Gateway. A tentar novamente...")
                                    self.connect_gateway()
                    
                # adiciona links à fila do Gateway
                for link in info["links"]:
                    if link not in self.urls_visitadas and self.validador_url(link):
                        for attempt in range(MAX_RETRIES):
                            try:
                                self.gateway_stub.putNew(index_pb2.PutNewRequest(url=link))
                                break  # sucesso, sai do loop de tentativas
                            except grpc.RpcError as e:
                                print(f"Erro ao conectar com o Gateway: {e}. A tentar novamente...")
                                time.sleep(3)  # espera antes de tentar novamente


            except grpc.RpcError as e:
                print(f"Erro RPC ao obter URL da fila: {e}.")
                self.connect_gateway()
                time.sleep(1)
                continue  

    def checkHealth(self, request, context):   
        """
        Método checkHealth:
            - Método simples para verificar se o barrel está ativo.
            - Retorna uma resposta vazia se o barrel estiver operacional.
        """

        return empty_pb2.Empty()
                       


def load_config(file_path='config.json'):
    """
    Função load_config:
        - Carrega a configuração do ficheiro JSON especificado.
        - Devolve o conteúdo do ficheiro como um dicionário Python.
    """

    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERRO: Ficheiro de configuração '{file_path}' não encontrado.")
        sys.exit(1)

if __name__ == "__main__":
    """
    Função main:
        - Ponto de entrada do programa.
        - Verifica argumentos, carrega configuração, inicia o servidor gRPC e o loop principal do Downloader.
    """

    # 1. Verifica e obtém o ID do argumento
    if len(sys.argv) < 2:
        print("ERRO: Uso correto é: python downloader.py <ID_DO_DOWNLOADER> (ex: downloader_1)")
        sys.exit(1)

    downloader_id = sys.argv[1] # O argumento 

    # 2. Carrega a configuração
    config = load_config('config.json')

    port = None

    for downloader_config in config['downloaders']:
        if downloader_config['id'] == downloader_id:
            port = downloader_config['port']
            break

    if port is None:
        print(f"ERRO: ID do Downloader '{downloader_id}' não encontrado na secção 'downloaders' do config.json.")
        sys.exit(1) 


    # obter endereço do gateway
    gateway_addr = f"{config['gateway']['ip']}:{config['gateway']['port']}"

    d = Downloader(gateway_addr)

    # 5. Inicia o Servidor gRPC e regista a instância Downloader
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=MAX_WORKERS))
    index_pb2_grpc.add_DownloaderServicer_to_server(d, server) 
    
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(f"Downloader Server/Crawler {downloader_id} iniciado na porta {port}")

    # 6. Inicia o Main Loop do Crawler (na thread principal)
    try:
        d.main_loop() 
    except KeyboardInterrupt:
        print("A encerrar o Downloader...")
    finally:
        server.stop(0)