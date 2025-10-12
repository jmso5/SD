from queue import Queue
from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
import grpc
import index_pb2
import index_pb2_grpc


class Downloader:
    def __init__(self, barrel_address = "localhost:8183"):
        self.urls_a_visitar = Queue()   # URLs por visitar
        self.urls_visitadas = set()     # URLs já visitados
        self.stub = self.connect_barrel(barrel_address)

    def connect_barrel(self, addr):
        canal = grpc.insecure_channel(addr)
        return index_pb2_grpc.IndexStub(canal)

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

    def processar_url(self, url):
        try:
            print(f"A descarregar: {url}")
            resposta = requests.get(url, timeout=5)

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

    '''def main_loop(self):
        """
        Loop principal (iterativo):
        Enquanto houver URLs na fila:
         - tira o próximo URL
         - faz o download
         - extrai informação
         - adiciona os links encontrados à fila (se ainda não visitados)
        """
        while not self.urls_a_visitar.empty():
            url = self.urls_a_visitar.get()

            # ignora se já tiver visitado (pode ter entrado por links duplicados)
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

            # atualiza índice invertido
            palavras = info["texto"].lower().split()

            for palavra in palavras:
                if palavra.isalpha():
                    if palavra not in self.indice_invertido:
                        self.indice_invertido[palavra] = set()
                    self.indice_invertido[palavra].add(info["url"])

            # adiciona novos links 
            for link in info["links"]:
                if link not in self.urls_visitadas:
                    self.urls_a_visitar.put(link)
    '''

    def main_loop(self, max_paginas=10):
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

    d.main_loop(max_paginas=10)

    termo = ""
    while termo.upper() != "SAIR":
        termo = input("\nPesquisar termo (ou ENTER para sair): ").strip()
        
        d.pesquisar(termo)
