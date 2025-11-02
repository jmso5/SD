from queue import Queue # fila FIFO para URLs
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
    def __init__(self, barrel_address, gateway_address):
        self.urls_visitadas = set()     # fila de URLs já visitados
        self.barrel_stub = self.connect_barrel(barrel_address) # conexão gRPC com o Barrel
        self.gateway_stub = self.connect_gateway(gateway_address) # conexão gRPC com o Gateway

        self.backlinks_map = {}  # dicionário para armazenar backlinks


    def connect_barrel(self, addrs): 
        """
        Conectar aos Barrels disponíveis.
        """

        stubs = []
        for addr in addrs:
            channel = grpc.insecure_channel(addr) # cria canal gRPC
            stub = index_pb2_grpc.StorageBarrelStub(channel) # stub para enviar mensagens
            stubs.append(stub)

        return stubs # devolve lista de stubs ativos
    

    def check_barrel_connections(self):  
        """
        Verifica periodicamente a conexão com os Barrels.
        """

        available_barrels = []

        for stub in self.barrel_stub:
            try:
                # tenta chamar um método simples para verificar se o barrel está ativo
                stub.checkHealth(empty_pb2.Empty()) # método simples para testar conexão
                available_barrels.append(stub) # se conseguir, mantém o stub
            except grpc.RpcError:
                continue

        return available_barrels
    
    def connect_gateway(self, addr):
        """
        Conectar ao Gateway.
        """

        while True:
            try:
                channel = grpc.insecure_channel(addr)
                stub = index_pb2_grpc.GatewayStub(channel) # stub para comunicar
                
                # Força uma chamada de teste (usando getStatistics, com timeout)
                # Isto força o gRPC a tentar estabelecer a conexão TCP/HTTP2.
                stub.checkHealth(empty_pb2.Empty()) 
                
                print(f"[Gateway] Conexão em {addr} estabelecida com sucesso.")
                return stub # Retorna o stub ativo
            
            except grpc.RpcError as e:
                # Se falhar a conexão ou der timeout
                print(f"[Gateway] Indisponível em {addr} (Erro: {e.code().name}). A tentar novamente em 5 segundos...")
            except Exception as e:
                # Erro inesperado
                print(f"[Gateway] Erro desconhecido durante a conexão: {e}. A tentar novamente em 5 segundos...")
                
            time.sleep(5) # Espera antes de tentar novamente
    

    def validador_url(self, url):
        """
        Valida URL.
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
        Faz o download da página.
        """

        try:
            print(f"A descarregar: {url}")

            # Define um User-Agent para evitar bloqueios por alguns servidores
            headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
            }
            resposta = requests.get(url,headers=headers, timeout=5) # faz pedido HTTP

            if resposta.status_code == 200 and 'text/html' in resposta.headers.get('Content-Type', ''): # resposta.status_code == 200 -> verifica se o pedido foi bem sucedido
                self.urls_visitadas.add(url) # marca URL como visitado
                return resposta.text
            else:
                print(f"Ignorado (status {resposta.status_code} ou não HTML): {url}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"Falha ao descarregar {url}: {e}")
            return None

    def extrair_informacao(self, html, base_url):
        """
        Extrai título, texto e links.
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
        Método RPC que responde ao Gateway/Cliente, devolvendo a lista de backlinks 
        para o URL solicitado, usando a cache local (self.backlinks_map).
        """
        url_original = request.url
        print(f"[Servidor RPC] Pedido de Backlinks para: {url_original}")

        try: 
            url_pesquisa_normalizada = normalize_url(url_original) # normaliza o URL para pesquisa

            backlinks_set = self.backlinks_map.get(url_pesquisa_normalizada, set()) # procura no dicionário local
            backlinks_list = list(backlinks_set)

            print(f"[Servidor RPC] Encontrados {len(backlinks_list)} backlinks para {url_original}")
            return index_pb2.BacklinksResponse(backlinks=backlinks_list)
            
        except Exception as e:
            print(f"Erro ao obter backlinks para {url_original}: {e}")
            # Em caso de erro, retorna uma resposta vazia
            return index_pb2.BacklinksResponse(backlinks=[])

    
    def main_loop(self):

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

                available_barrels = self.check_barrel_connections() # verifica conexões com barrels

                if not available_barrels:
                    print("Nenhum Barrel disponível. A aguardar 3 segundos antes de tentar novamente...")
                    time.sleep(3)
                    continue

                # envia palavras para o Barrel
                palavras = info["texto"].lower().split()
                for palavra in palavras:
                    if palavra.isalpha(): # ignora números e símbolos
                        for stub in available_barrels: # para cada barrel ativo
                            for attempt in range (MAX_RETRIES):
                                try:
                                    links = []
                                    for link in info['links']:
                                        links.append(link)

                                    # chama o método remoto no barrel
                                    stub.addToIndex(
                                        index_pb2.AddToIndexRequest(
                                            word=palavra,
                                            url=info["url"],
                                            title=info["titulo"],
                                            citacao=info["texto"][:200],  
                                            links_count=len(info['links'])
                                        )
                                    )
                                except grpc.RpcError as e:
                                    print(f"Erro RPC: {e}")
                
                # atualiza dicionário de backlinks
                for link_destino in info["links"]:
                    link_valido = self.validador_url(link_destino)
                    if link_valido:
                        link_normalizado = normalize_url(link_destino)
                        if link_normalizado not in self.backlinks_map:
                            self.backlinks_map[link_normalizado] = set()

                        self.backlinks_map[link_normalizado].add(url) # regista origem do link

                # adiciona links à fila do Gateway
                for link in info["links"]:
                    if link not in self.urls_visitadas and self.validador_url(link):
                        for attempt in range(MAX_RETRIES):
                            try:
                                #self.queue_stub.putNew(index_pb2.PutNewRequest(url=link))
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
                       
    
def normalize_url(url):
    """
    Normaliza um URL para apenas o domínio
    """
    try:
        parsed = urlparse(url)
        hostname = parsed.netloc.lower()
        
        if hostname.startswith("www."):
            hostname = hostname[4:]
        
        # Mantém apenas o domínio, com esquema HTTPS padronizado
        # Note que o path será vazio (''), o que faz a chave ser apenas 'https://dominio.com'
        normalized_url = urlunparse(('https', hostname, '', '', '', '')) 
        return normalized_url.lower()

    except Exception:
        return url.lower()


def load_config(file_path='config.json'):
    """
    Carrega o ficheiro de configuração.
    """

    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERRO: Ficheiro de configuração '{file_path}' não encontrado.")
        sys.exit(1)

if __name__ == "__main__":

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

    # obter endereços dos barrels
    barrel_addrs = []
    for barrel_config in config['barrels']:
        barrel_addrs.append(f"{barrel_config['ip']}:{barrel_config['port']}") 

    # obter endereço do gateway
    gateway_addr = f"{config['gateway']['ip']}:{config['gateway']['port']}"

    d = Downloader(barrel_addrs, gateway_addr)

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