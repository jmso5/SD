from concurrent import futures
import grpc
from google.protobuf import empty_pb2
import index_pb2
import index_pb2_grpc
from queue import Queue
import pickle
import os

class IndexServicer(index_pb2_grpc.IndexServicer, index_pb2_grpc.QueueServicer):
    def __init__(self, ficheiro_fila="urls_queue.pkl"):
        self.indice_invertido = {}

        # ---- Fila de URLs (ex-QueueServicer) ----
        self.fila = Queue()
        self.urls_guardados = set()
        self.ficheiro_fila = ficheiro_fila
        self.carregar_fila()

    # ------------------ INDEX PART ------------------

    def addToIndex(self, request, context):
        palavra = request.word.lower()
        url = request.url
        title = request.url
        citacao = request.url

        if palavra not in self.indice_invertido:
            self.indice_invertido[palavra] = set()
        self.indice_invertido[palavra].add((url, title, citacao))

        print(f"[+] Adicionada '{palavra}' -> {url}")
        return empty_pb2.Empty()

    def searchWord(self, request, context):
        termo = request.word.lower()
        resultados = [
            index_pb2.SearchResult(url=url, title=title, citacao=citacao)
            for url, title, citacao in self.indice_invertido.get(termo, [])
        ]
        print(f"Pesquisa '{termo}' -> {len(resultados)} resultados")
        return index_pb2.SearchWordResponse(results=resultados)

    # ------------------ QUEUE PART ------------------

    def carregar_fila(self):
        if os.path.exists(self.ficheiro_fila):
            try:
                with open(self.ficheiro_fila, "rb") as f:
                    urls = pickle.load(f)
                    for url in urls:
                        self.fila.put(url)
                        self.urls_guardados.add(url)
                print(f"[✓] Fila carregada com {len(self.urls_guardados)} URLs.")
            except Exception as e:
                print(f"[!] Erro ao carregar fila: {e}")

    def guardar_fila(self):
        try:
            with open(self.ficheiro_fila, "wb") as f:
                pickle.dump(list(self.urls_guardados), f)
            print(f"[✓] Fila guardada ({len(self.urls_guardados)} URLs).")
        except Exception as e:
            print(f"[!] Erro ao guardar fila: {e}")

    def putNew(self, request, context):
        url = request.url.strip()
        if url and url not in self.urls_guardados:
            self.fila.put(url)
            self.urls_guardados.add(url)
            print(f"[+] URL adicionado à fila: {url}")
            self.guardar_fila()
        else:
            print(f"[!] URL duplicado ou inválido: {url}")
        return empty_pb2.Empty()

    def takeNext(self, request, context):
        if not self.fila.empty():
            url = self.fila.get()
            print(f"[→] Entregue ao downloader: {url}")
            self.guardar_fila()
            return index_pb2.TakeNextResponse(url=url)
        else:
            print("[!] Fila vazia.")
            return index_pb2.TakeNextResponse(url="")

# ------------------ SERVIDOR ------------------

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    servico = IndexServicer()
    index_pb2_grpc.add_IndexServicer_to_server(servico, server)
    index_pb2_grpc.add_QueueServicer_to_server(servico, server)

    server.add_insecure_port('[::]:8183')
    server.start()
    print("Barrel (Index + Queue) iniciado na porta 8183")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("A encerrar o Barrel...")
        server.stop(0)

if __name__ == '__main__':
    serve()
