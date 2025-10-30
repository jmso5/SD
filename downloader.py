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

MAX_RETRIES = 3

class Downloader:
    def __init__(self, barrel_address, gateway_address):
        self.urls_a_visitar = Queue()   # fila de URLs por visitar
        self.urls_visitadas = set()     # fila de URLs já visitados
        self.barrel_stub = self.connect_barrel(barrel_address) # conexão gRPC com o Barrel
        self.gateway_stub = self.connect_gateway(gateway_address)         # para takeNext e putNew

    # alteração para conectar todos os barrels
    def connect_barrel(self, addrs):
        stubs = []
        for addr in addrs:
                channel = grpc.insecure_channel(addr) 
                stubs.append(index_pb2_grpc.StorageBarrelStub(channel))
        return stubs
    
    def connect_gateway(self, addr):
        channel = grpc.insecure_channel(addr)
        return index_pb2_grpc.GatewayStub(channel)
    
    def adicionar_url_inicial(self, url):
        """
        Adiciona o primeiro URL à fila.
        (isto é o que o user insere manualmente)
        """
        if url not in self.urls_visitadas:
            print(f"URL adicionado à fila: {url}")
            self.urls_a_visitar.put(url)
        else:
            print(f"URL já visitado: {url}")
    
    def validador_url(self, url):
        # Filtra URLs inválidos

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
        try:
            print(f"A descarregar: {url}")

            # Define um User-Agent para evitar bloqueios por alguns servidores
            headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
            }
            resposta = requests.get(url,headers=headers, timeout=5)

            if resposta.status_code == 200 and 'text/html' in resposta.headers.get('Content-Type', ''): # resposta.status_code == 200 -> verifica se o pedido foi bem sucedido
                self.urls_visitadas.add(url)
                return resposta.text
            else:
                print(f"Ignorado (status {resposta.status_code} ou não HTML): {url}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"Falha ao descarregar {url}: {e}")
            return None

    def extrair_informacao(self, html, base_url):
        soup = BeautifulSoup(html, "html.parser")

        titulo = soup.title.string if soup.title else "Sem título"
        texto = soup.get_text(separator=" ", strip=True)
        links = []

        for a in soup.find_all("a", href=True):
            link_absoluto = urljoin(base_url, a["href"])  # converte link relativo para absoluto
            links.append(link_absoluto)

        return {"url": base_url, "titulo": titulo, "texto": texto, "links": links}
    
    def getBacklinks(self, request, context):
        """
        Método que obtém os backlinks de um URL.
        Apresenta a lista de URLs que apontam para o URL pedido.
        """
        url = request.url

        try: 
            html = self.processar_url(url)
            info = self.extrair_informacao(html, url)

            links_list = info["links"]

            return index_pb2.BacklinksResponse(backlinks=links_list)
        except:
            print(f"Erro ao obter backlinks para {url}")
            return []
            

    
    def main_loop(self):

        while True:

            try:     
                resposta = self.gateway_stub.takeNext(empty_pb2.Empty())
                url = resposta.url
                
                if not url:
                    print("[Fila] vazia, a esperar...")
                    time.sleep(3)
                    continue

                if url in self.urls_visitadas:
                    continue

                html = self.processar_url(url)
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
                    if palavra.isalpha():
                        for stub in self.barrel_stub:
                            for attempt in range (MAX_RETRIES):
                                try:
                                    links = []
                                    for link in info['links']:
                                        links.append(link)

                                    stub.addToIndex(
                                        index_pb2.AddToIndexRequest(
                                            word=palavra,
                                            url=info["url"],
                                            title=info["titulo"],
                                            citacao=info["texto"][:200],  # primeiros 200 carateres da página
                                            links_count=len(info['links'])
                                        )
                                    )
                                except grpc.RpcError as e:
                                    print(f"Erro RPC: {e}")
            

                # Adiciona links encontrados à Queue
                for link in info["links"]:
                    if link not in self.urls_visitadas and self.validador_url(link):
                        try:
                            #self.queue_stub.putNew(index_pb2.PutNewRequest(url=link))
                            self.gateway_stub.putNew(index_pb2.PutNewRequest(url=link))
                        except grpc.RpcError as e:
                            print(f"Erro RPC ao adicionar URL à Queue: {e}")

            except grpc.RpcError as e:
                print(f"Erro RPC ao obter URL da fila: {e}. A aguardar 10 segundos...")
                time.sleep(10)
                continue                
    


def load_config(file_path='config.json'):
    """Carrega o ficheiro de configuração."""
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
    d.main_loop()