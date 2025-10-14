from queue import Queue # fila FIFO para URLs
from bs4 import BeautifulSoup # Biblioteca para parsear HTML e extrair texto/links
import requests # Para fazer o download das páginas
from urllib.parse import urljoin # Para transformar links relativos em absolutos
import grpc # Comunicação RPC com o Barrel
import index_pb2
import index_pb2_grpc


class Downloader:
    def __init__(self, barrel_address = "localhost:8183"):
        self.urls_a_visitar = Queue()   # fila de URLs por visitar
        self.urls_visitadas = set()     # fila de URLs já visitados
        self.stub = self.connect_barrel(barrel_address) # conexão gRPC com o Barrel

    def connect_barrel(self, addr):
        canal = grpc.insecure_channel(addr)  # cria canal gRPC
        return index_pb2_grpc.IndexStub(canal)  # cria stub (cliente)

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


    """
    def main_loop(self, max_paginas=1000):
        paginas_processadas = 0
        while not self.urls_a_visitar.empty() and paginas_processadas < max_paginas:
            url = self.urls_a_visitar.get()
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
                    try:
                        self.stub.addToIndex(index_pb2.AddToIndexRequest(word=palavra, url=info["url"]))
                    except grpc.RpcError as e:
                        print(f"Erro RPC: {e}")

            paginas_processadas += 1
    """

    def main_loop(self):
        while not self.urls_a_visitar.empty():
            url = self.urls_a_visitar.get()
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
                    try:
                        self.stub.addToIndex(index_pb2.AddToIndexRequest(word=palavra, url=info["url"]))
                    except grpc.RpcError as e:
                        print(f"Erro RPC: {e}")

            # adiciona novos links à fila
            for link in info["links"]:
                if link not in self.urls_visitadas and self.validador_url(link):
                    self.urls_a_visitar.put(link)



    def pesquisar(self, termo):
        try:
            resposta = self.stub.searchWord(index_pb2.SearchWordRequest(word=termo))
            if resposta.urls:
                print(f"\nResultados para '{termo}':")
                for url in resposta.urls:
                    print(f" - {url}")
            else:
                print(f"\nNenhum resultado para '{termo}'.")
        except grpc.RpcError as e:
            print(f"Erro ao pesquisar: {e}")


if __name__ == "__main__":
    d = Downloader()

    url_introduzido = input("Instroduza URL: ") 
    d.adicionar_url_inicial(url_introduzido)

    #d.main_loop(max_paginas=10)
    d.main_loop()

    termo = ""

    while (True):
        termo = input("\nPesquisar termo (ou ENTER para sair): ").strip()

        if termo.lower() == "sair":
            print("A sair...")
            break
        
        d.pesquisar(termo)
