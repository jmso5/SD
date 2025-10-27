from concurrent import futures
import grpc
from google.protobuf import empty_pb2
import index_pb2
import index_pb2_grpc
from queue import Queue
import pickle
import os
import sys 
import json

class BarrelServicer(index_pb2_grpc.StorageBarrelServicer, index_pb2_grpc.GatewayServicer):
    def __init__(self, barrel_id, replica_barrels, ficheiro_fila="urls_queue.pkl"):
        self.barrel_id = barrel_id
        self.indice_invertido = {}

        # --- Conexão com réplicas de Barrels ---
        self.replica_stubs = self.connect_replica_barrels(replica_barrels)
        print(f"[Iniciado] Barrel com {len(self.replica_stubs)} réplicas conectadas.")

        # ---- Fila de URLs (ex-QueueServicer) ----
        self.fila = Queue()
        self.urls_guardados = set() 
        self.ficheiro_fila = ficheiro_fila
        self.carregar_fila()
    

    def connect_replica_barrels(self, addresses):
        stubs = []
        for addr in addresses:
            channel = grpc.insecure_channel(addr)
            stub = index_pb2_grpc.StorageBarrelStub(channel)
            stubs.append(stub)
        return stubs

    # ------------------ INDEX PART ------------------

    def addToIndex(self, request, context):
        palavra = request.word.lower()
        url = request.url
        title = request.title
        citacao = request.citacao
        links_count = request.links_count

        novo_registo = (url, title, citacao, links_count)

        if palavra not in self.indice_invertido:
            #self.indice_invertido[palavra] = set()
            self.indice_invertido[palavra] = []
        #self.indice_invertido[palavra].add((url, title, citacao))
        self.indice_invertido[palavra].append(novo_registo)

        print(f"[Adicionada] '{palavra}' -> {url} (Revelânica: {links_count})")
        return empty_pb2.Empty()

    def searchWord(self, request, context):
        termo = request.word.lower()
        resultados = []

        for temo in self.indice_invertido:
            resultados_ordenados = sorted(
                self.indice_invertido[termo],
                key = lambda x: x[3],
                reverse= True
            )
            for url, title, citacao, links_count in resultados_ordenados:
                resultados.append(
                    index_pb2.SearchResult(
                        url=url,
                        title=title,
                        text=citacao,
                        links_count = links_count
                    )
                )

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
                print(f"[Fila carregada] com {len(self.urls_guardados)} URLs.")
            except Exception as e:
                print(f"[Erro] ao carregar fila: {e}")

    def guardar_fila(self):
        try:
            with open(self.ficheiro_fila, "wb") as f:
                pickle.dump(list(self.urls_guardados), f)
            print(f"[Fila guardada] ({len(self.urls_guardados)} URLs).")
        except Exception as e:
            print(f"[Erro] ao guardar fila: {e}")

    def putNew(self, request, context):
        url = request.url.strip()

        if url and url not in self.urls_guardados:
            self.fila.put(url)
            self.urls_guardados.add(url)
            print(f"[Adicionado] URL à fila: {url}")
            self.guardar_fila()

            for stub in self.replica_barrels:
                try:
                    stub.putNew(request)
                    print(f"[Replicado] URL para réplica: {url}")
                except grpc.RpcError as e:
                    print(f"[Erro] ao replicar URL para réplica: {e}")
        else:
            print(f"[URL] duplicado ou inválido: {url}")
        return empty_pb2.Empty()

    def takeNext(self, request, context):
        if not self.fila.empty():
            url = self.fila.get()

            if url in self.urls_guardados:
                self.urls_guardados.remove(url)
                

            print(f"[Entregue] ao downloader: {url}")
            self.guardar_fila()

            for stub in self.replica_barrels:
                try:
                    stub.takeNext(request)
                    print(f"[Replicado] remoção de URL para réplica: {url}")
                except grpc.RpcError as e:
                    print(f"[Erro] ao replicar remoção de URL para réplica: {e}")


            return index_pb2.TakeNextResponse(url=url)
        else:
            print("[Fila vazia].")
            return index_pb2.TakeNextResponse(url="")

# ------------------ SERVIDOR ------------------
def server(port, barrel_id):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))


    replica_barrels = []
    config = load_config()
    for barrel_config in config['barrels']:
        if barrel_config['id'] != barrel_id:
            replica_barrels.append(f"{barrel_config['ip']}:{barrel_config['port']}")
    

    servico = BarrelServicer(barrel_id, replica_barrels)
    index_pb2_grpc.add_StorageBarrelServicer_to_server(servico, server)
    index_pb2_grpc.add_GatewayServicer_to_server(servico, server)

    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(f"Barrel {barrel_id} iniciado na porta {port}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("A encerrar o Barrel...")
        server.stop(0)

def load_config(file_path='config.json'):
    """Carrega o ficheiro de configuração."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERRO: Ficheiro de configuração '{file_path}' não encontrado.")
        sys.exit(1)

    
if __name__ == '__main__':
    # 1. Verifica e obtém o ID do argumento
    if len(sys.argv) < 2:
        print("ERRO: Uso correto é: python barrel.py <ID_DO_BARREL> (ex: barrel_1)")
        sys.exit(1)
    
    barrel_id = sys.argv[1] # O argumento (ex: 'barrel_1')
    
    # 2. Carrega a configuração
    config = load_config()
    
    port = None
    
    # 3. Procura a porta que corresponde ao ID no config.json
    for barrel_config in config['barrels']:
        if barrel_config['id'] == barrel_id:
            port = barrel_config['port']
            break

    if port is None:
        print(f"ERRO: ID do Barrel '{barrel_id}' não encontrado na secção 'barrels' do config.json.")
        sys.exit(1)

    server(port, barrel_id)