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
import random
import time

MAX_MESSAGE_LENGTH = 104857600 #50 * 1024 * 1024 

class BarrelServicer(index_pb2_grpc.StorageBarrelServicer, index_pb2_grpc.GatewayServicer):
    def __init__(self, barrel_id, replica_barrels, ficheiro_fila="urls_queue.pkl", ficheiro_indice="inverted_index.pkl", ficheiro_visitados="visited_urls.pkl"):
        self.barrel_id = barrel_id # id do barrel 
        self.indice_invertido = {} # estrutura do índice invertido

        self.replica_stubs = self.connect_replica_barrels(replica_barrels) # conecta às réplicas dos barrels
        print(f"[Iniciado] Barrel com {len(self.replica_stubs)} réplicas conectadas.")

        available_replicas = []
        available_replicas = self.check_available_replicas() # verifica réplicas disponíveis

        # gestão na fila local de URLs
        self.fila = Queue()
        self.urls_guardados = set() # evita urls duplicados
        self.ficheiro_fila = ficheiro_fila
        self.ficheiro_indice = ficheiro_indice
        self.ficheiro_visitados = ficheiro_visitados

        if available_replicas == []:
            print("[ATENÇÃO] Nenhuma réplica de Barrel está disponível no momento.")
            self.carregar_dados() # carrega dados dos ficheiros, se existir

        else:
            print(f"[Info] Réplicas disponíveis: {len(available_replicas)}. Sincronizando fila de URLs...")
        
            # sincroniza a fila com a primeira réplica disponível
            replica_stub = random.choice(available_replicas)
            try:
                replica_stub.guardar_dados_replica(empty_pb2.Empty()) # pede à réplica para carregar os dados

                self.carregar_dados()

                print(f"[Sincronizado] Fila de URLs sincronizada com réplica. URLs na fila: {self.fila.qsize()}.")
                print(f"[Sincronizado] Indice invertido sincronizado com réplica. Entradas no índice: {len(self.indice_invertido)}.")
                print(f"[Sincronizado] Urls visitadas sincronizadas com réplica. URLs visitadas: {len(self.urls_guardados)}.")
            
            except grpc.RpcError as e:
                print(f"[Erro] ao sincronizar fila com réplica: {e}")
                
    

    def connect_replica_barrels(self, addresses):
        """
        Conecta às réplicas dos barrels e retorna uma lista de stubs.
        """
        stubs = []

        options = [
            ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
            ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
        ]

        for addr in addresses:
            channel = grpc.insecure_channel(addr, options=options) # cria canal gRPC
            stub = index_pb2_grpc.StorageBarrelStub(channel) # cria stub remoto
            stubs.append(stub)
        return stubs
    
    def check_available_replicas(self): 
        """
        Verifica quais réplicas estão disponíveis.
        """
        available_stubs = []
        for stub in self.replica_stubs:
            try:
                stub.checkHealth(empty_pb2.Empty()) # chamada simples para verificar disponibilidade
                available_stubs.append(stub)
            except grpc.RpcError:
                print("[Replica] Réplica indisponível.")
        self.replica_stubs = available_stubs

        return available_stubs
  

    def addToIndex(self, request, context):
        """
        Adiciona uma entrada ao índice invertido.
        """

        palavra = request.word.lower() # normaliza palavra
        url = request.url
        title = request.title
        citacao = request.citacao
        links_count = request.links_count

        novo_registo = (url, title, citacao, links_count) # estrutura do registo

        if palavra not in self.indice_invertido:
            self.indice_invertido[palavra] = [] # cria nova entrada

        # evita duplicados    
        for registo in self.indice_invertido[palavra]:
            if registo[0] == url:
                return empty_pb2.Empty() 

        self.indice_invertido[palavra].append(novo_registo) # adiciona ao índice

        print(f"[Adicionada] '{palavra}' -> {url} (Revelânica: {links_count})")
        return empty_pb2.Empty()

    def searchWord(self, request, context):
        """
        Pesquisa uma palavra no índice invertido e retorna os resultados.
        """

        termo = request.word.lower()
        resultados = []

        if termo in self.indice_invertido:
            # ordena os resultados por relevância (número de links)
            resultados_ordenados = sorted(
                self.indice_invertido[termo],
                key = lambda x: x[3],
                reverse= True
            )

            # converte para formato protobuf
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
    

    def searchVariousWords(self, request, context):

        word_details = {}
        for word in request.words:
            termo = word.lower()
            if termo in self.indice_invertido:
                for url, title, citacao, links_count in self.indice_invertido[termo]:
                    if url not in word_details:
                        word_details[url] = {
                            'url': url,
                            'title': title,
                            'citacao': citacao,
                            'links_count': links_count,
                            'count': 1  # número de palavras encontradas neste URL
                        }
                    else:
                        print(f"Incrementing count for URL: {url}")
                        word_details[url]['count'] += 1
    
        resultados = []
        for details in word_details.values():
            # Apenas inclui resultados que contêm todas as palavras pesquisadas
            if details['count'] == len(request.words): 
                resultados.append(
                    index_pb2.SearchResult(
                        url=details['url'],
                        title=details['title'],
                        text=details['citacao'],
                        links_count=details['links_count']
                    )
                )
        # ordena os resultados pelo número de palavras encontradas e depois por relevância
        resultados.sort(key=lambda x: (word_details[x.url]['count'], x.links_count), reverse=True)

        print(f"Pesquisa várias palavras '{', '.join(request.words)}' -> {len(resultados)} resultados")
        return index_pb2.SearchWordResponse(results=resultados)

    def carregar_dados(self):
        """
        Carrega a fila de URLs a partir de um ficheiro.
        """

        if os.path.exists(self.ficheiro_indice): # verifica se o ficheiro existe
            try:
                with open(self.ficheiro_indice, "rb") as f:
                    self.indice_invertido = pickle.load(f) # deserializa o conjunto de URLs
                print(f"[Indice Invertido carregado] com {len(self.indice_invertido)} URLs.")
            except Exception as e:
                print(f"[Erro] ao carregar Indice Invertido: {e}")

        if os.path.exists(self.ficheiro_fila): # verifica se o ficheiro existe
            try:
                with open(self.ficheiro_fila, "rb") as f:
                    fila = pickle.load(f) # deserializa o conjunto de URLs
                    for url in fila:
                        self.fila.put(url)

                print(f"[Fila carregada] com {len(self.fila)} URLs.")
            except Exception as e:
                print(f"[Erro] ao carregar fila: {e}")

        if os.path.exists(self.ficheiro_visitados): # verifica se o ficheiro existe
            try:
                with open(self.ficheiro_visitados, "rb") as f:
                    self.urls_guardados = pickle.load(f) # deserializa o conjunto de URLs visitadas
                print(f"[Urls visitados carregadas] com {len(self.urls_guardados)} URLs.")
            except Exception as e:
                print(f"[Erro] ao carregar urls visitados: {e}")


    def guardar_dados_replica(self, request, context):
        """
        Guarda a fila de URLs num ficheiro (chamado pela réplica).
        """
        self.guardar_dados()
        time.sleep(1) # espera 1 segundo para garantir que os dados são escritos           VERIFICAR

        return empty_pb2.Empty()

    def guardar_dados(self):
        """
        Guarda a fila de URLs num ficheiro.
        """

        try:
            with open(self.ficheiro_indice, "wb") as f:
                pickle.dump(self.indice_invertido, f) # guarda urls num ficheiro
            print(f"[Indice invertido guardado]")
        except Exception as e:
            print(f"[Erro] ao guardar indice invertido: {e}")

        try:
            with open(self.ficheiro_visitados, "wb") as f:
                pickle.dump(self.urls_guardados, f) # guarda urls num ficheiro
            print(f"[Urls visitadas guardadas] ({len(self.urls_guardados)} URLs).")
        except Exception as e:
            print(f"[Erro] ao guardar urls visitadas: {e}")

        try:
            with open(self.ficheiro_fila, "wb") as f:
                pickle.dump(list(self.fila.queue), f) # guarda urls num ficheiro
            print(f"[Fila guardada] ({self.fila.qsize()} URLs).")
        except Exception as e: 
            print(f"[Erro] ao guardar fila: {e}")

    
    def checkHealth(self, request, context):    
        return empty_pb2.Empty()
        

    def putNew(self, request, context):
        """
        Adiciona um novo URL à fila de URLs a visitar.
        """

        url = request.url

        if url not in self.urls_guardados:
            self.fila.put(url) # adiciona à fila
            
            print(f"[Fila] Adicionado novo URL: {url}. URLs totais: {self.fila.qsize()}")

            available_replicas = self.check_available_replicas()

            # Replica o putNew para as réplicas dos barrels
            sucessos = 0
            for stub in available_replicas:
                max_retries = 3 # tenta 3 vezes contactar esta réplica
                for attempt in range(max_retries):
                    try:
                        stub.putNew(request) # envia o mesmo pedido para a réplica
                        sucessos += 1
                        print(f"[Replica] Sucesso em putNew para réplica (tentativa {attempt + 1}).")
                        break # sai do loop de retries -> sucesso para esta réplica
                    except grpc.RpcError as e:
                        print(f"[Replica] Falha ao replicar putNew para réplica (tentativa {attempt + 1}): {e}")
                        
            # o Barrel não falha se nenhuma réplica estiver ativa, mas imprime um aviso
            if sucessos == 0 and self.replica_stubs:
                print("[ATENÇÃO] NENHUMA réplica Barrel recebeu o putNew.")

        return empty_pb2.Empty()

    def takeNext(self, request, context):    
        """
        Retira o próximo URL da fila de URLs a visitar.
        """    
        
        try:
            url = self.fila.get_nowait() # retira o próximo URL da fila
            self.urls_guardados.add(url) # marca como visitado

            print(f"[Fila] Entregue URL: {url}. Restam {self.fila.qsize()} URLs.")

            available_replicas = self.check_available_replicas()   

            for stub in available_replicas:
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        stub.takeProcessedUrl(empty_pb2.Empty()) # notifica a réplica para retirar o próximo URL
                        print(f"[Replica] Sucesso em takeNext para réplica (tentativa {attempt + 1}).")
                        break
                    except grpc.RpcError as e:
                        print(f"[Replica] Falha ao replicar takeNext para réplica (tentativa {attempt + 1})")

            return index_pb2.TakeNextResponse(url=url)

        except Exception as e:
            print(f"Erro ao retirar URL da fila: {e}")
            return index_pb2.TakeNextResponse(url="")
        
    def takeProcessedUrl(self, request, context):
        for url in self.fila:
            if url == request.url:
                self.fila.remove(url)
                self.guardar_fila()
                print(f"[Fila] URL processado removido: {url}. Restam {self.fila.qsize()} URLs.")
                break
        
    def getIndexSize(self, request, context):
        """
        Retorna o tamanho do índice invertido (número de palavras únicas indexadas).
        """
        return index_pb2.BarrelSizeResponse(size=len(self.indice_invertido))

def server(port, barrel_id):
    """
    Inicia o servidor gRPC do Barrel.
    """

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
            ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
        ]
    )

    replica_barrels = []
    config = load_config()
    for barrel_config in config['barrels']:
        if barrel_config['id'] != barrel_id:
            replica_barrels.append(f"{barrel_config['ip']}:{barrel_config['port']}") # lista de réplicas
    
    servico = BarrelServicer(barrel_id, replica_barrels)
    index_pb2_grpc.add_StorageBarrelServicer_to_server(servico, server)
    index_pb2_grpc.add_GatewayServicer_to_server(servico, server)

    server.add_insecure_port(f"0.0.0.0:{port}") # escuta em todas as interfaces
    server.start()
    print(f"Barrel {barrel_id} iniciado na porta {port}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        servico.guardar_dados() # guarda a fila antes de encerrar
        print("A encerrar o Barrel...")
        server.stop(0)

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
    
if __name__ == '__main__':
    # 1. Verifica e obtém o ID do argumento
    if len(sys.argv) < 2:
        print("ERRO: Uso correto é: python barrel.py <ID_DO_BARREL> (ex: barrel_1)")
        sys.exit(1)
    
    barrel_id = sys.argv[1] # ID do barrel a iniciar
    
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

    server(port, barrel_id) # incia o servidor gRPC 