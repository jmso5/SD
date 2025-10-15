import grpc
from concurrent import futures
from google.protobuf import empty_pb2
import index_pb2
import index_pb2_grpc
import pickle
import os
from queue import Queue


class QueueServicer(index_pb2_grpc.QueueServicer):
    def __init__(self, ficheiro_fila="urls_queue.pkl"):
        # A fila de URLs e a persistência em ficheiro
        self.fila = Queue()
        self.urls_guardados = set()
        self.ficheiro_fila = ficheiro_fila
        self.carregar_fila()

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

    # -----------------------------
    # Implementações RPC
    # -----------------------------
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

    # Estes dois métodos são necessários porque herdamos IndexServicer,
    # mas não queremos usá-los aqui (são usados no Barrel).
    def addToIndex(self, request, context):
        return empty_pb2.Empty()

    def searchWord(self, request, context):
        return index_pb2.SearchWordResponse(urls=[])


def serve():
    # só para ver com que urls começamos o programa
    '''
    servico = QueueServicer()
    print(f"[DEBUG] URLs carregados na inicialização:")
    for url in servico.urls_guardados:
        print(f" - {url}")
    print(f"Total: {len(servico.urls_guardados)} URLs na fila.\n")
    '''

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    index_pb2_grpc.add_QueueServicer_to_server(QueueServicer(), server)
    server.add_insecure_port("[::]:8182")
    server.start()
    print("Servidor de URL Queue iniciado na porta 8182")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("A encerrar o Queue...")
        server.stop(0)


if __name__ == "__main__":
    serve()
