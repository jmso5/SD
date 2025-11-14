"""
Módulo Barrel

    - O Barrel atua como um nó de armazenamento de dados e de indexação para o
    sistema de busca. É responsável por armazenar o índice invertido e gerir a fila de URLs a visitar.

    - Funcionalidades principais:
        - Armazenamento do índice invertido: Mantém um índice que mapeia palavras-chave para URLs
          onde essas palavras aparecem, juntamente um título, citação e número de links.
        - Gestão da fila de URLs: Mantém uma fila de URLs a visitar, permitindo adicionar novos URLs
          e retirar o próximo URL para processamento.
        - Replicação de dados: Replica os dados recebidos e os processados para os outros barrels
          para garantir a disponibilidade e resiliência dos dados.
        - Comunicação gRPC: Expõe métodos gRPC para interagir com outros componentes do sistema,
          como o Gateway e outras réplicas de barrels.

    - Mecanismos de Sincronização:
        - Ao iniciar, o Barrel verifica a disponibilidade das suas réplicas. Se encontrar réplicas disponíveis, sincroniza a sua fila de URLs e índice invertido
          com a primeira réplica disponível. Se nenhuma réplica estiver disponível, carrega os dados locais a partir de ficheiros
          previamente guardados.
        - As operações de comidicação de estado (putNew, takeNext, addToIndex) são replicadas para todas as réplicas disponíveis, mantendo a consistência dos dados.


    - Estrutura do indice invertido:
        {
            "palavra1": [
                (url1, title1, citacao1, links_count1),
                (url2, title2, citacao2, links_count2),
                ...
            ],
            "palavra2": [
                (url3, title3, citacao3, links_count3),
                ...
            ],
            ...
        }
"""


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
    """
    Servidor gRPC que atua como um Barrek, providenciando serviços de indexação , pesquisa e gerenciamento da fila de URLs.

    Atributos:
        - barrel_id: Identificador único do barrel.
        - indice_invertido: Dicionário que representa o índice invertido.
        - fila: Fila de URLs a visitar.
        - urls_guardados: Conjunto de URLs já visitadas.
        - replica_stubs: Lista de stubs para comunicação com réplicas de barrels.
        - ficheiro_fila: Nome do ficheiro para armazenar a fila de URLs.
        - ficheiro_indice: Nome do ficheiro para armazenar o índice invertido.
        - ficheiro_visitados: Nome do ficheiro para armazenar as URLs visitadas.
    """

    def __init__(self, barrel_id, replica_barrels, ficheiro_fila="urls_queue.pkl", ficheiro_indice="inverted_index.pkl", ficheiro_visitados="visited_urls.pkl"):
        """
        Inicializa o Barrel, estabelecendo conexões com réplicas e carregando dados se possível.
    
        """
        
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
            except grpc.RpcError as e:
                print(f"[Erro] ao sincronizar fila com réplica: {e}")
                
    

    def connect_replica_barrels(self, addresses):
        """
        Método connect_replica_barrels:
            - Estabelece conexões gRPC com as réplicas dos barrels.
            - Retorna uma lista de stubs para comunicação com as réplicas.
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
        Método check_available_replicas:
            - Verifica a disponibilidade das réplicas dos barrels.
            - Utiliza uma chamada simples para verificar se a réplica está ativa (checkHealth).
            - Retorna uma lista de stubs das réplicas disponíveis.
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
        Método addToIndex:
                - Se palavra ainda não existir no índice, cria uma nova entrada.
                - Se o URL já existir para a palavra, ignora o pedido para evitar duplicados.
                - Adiciona o novo registo (URL, título, citação, número de links) ao índice invertido.
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
        Método searchWord:
            - Pesquisa uma palavra no índice invertido.
            - Ordena os resultados por relevância.
            - Converte os resultados para o formato protobuf.
            - Retorna uma lista de resultados ordenados por relevância (número de links).
        """

        termo = request.word.lower()
        resultados = []

        if termo in self.indice_invertido:
            # ordena os resultados por relevância
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
        """
        Método searchVariousWords:
            - Pesquisa várias palavras no índice invertido.
            - Conta quantas palavras de pesquisa aparecem em cada URL.
            - Filtra os resultados para incluir apenas URLs que contêm todas as palavras pesquisadas.
            - Ordena os resultados pelo número de palavras encontradas e pela relevância.
            - Converte os resultados para o formato protobuf.
            - Retorna uma lista de resultados ordenados.
        """


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
        Método carregar_dados:
            - Carrega a fila de URLs, o índice invertido e as URLs visitadas a partir de ficheiros.
            - Utiliza pickle para desserializar os dados.
            - Verifica se os ficheiros existem antes de tentar carregá-los.
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

                print(f"[Fila carregada] com {self.fila.qsize()} URLs.")
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
        Método guardar_dados_replica:
            - Guarda a fila de URLs, o índice invertido e as URLs visitadas em ficheiros.
            - Serve apenas para ser chamado pelasmas réplicas dos barrels. 
        """

        self.guardar_dados()
        time.sleep(1) # espera 1 segundo para garantir que os dados são escritos           VERIFICAR

        return empty_pb2.Empty()

    def guardar_dados(self):
        """
        Método guardar_dados:
            - Guarda a fila de URLs, o índice invertido e as URLs visitadas em ficheiros.
            - Utiliza pickle para serializar os dados.
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
        """
        Método checkHealth:
            - Método simples para verificar se o barrel está ativo.
            - Retorna uma resposta vazia se o barrel estiver operacional.
        """

        return empty_pb2.Empty()
        

    def putNew(self, request, context):
        """
        Método putNew:
            - Adiciona um novo URL à fila de URLs a visitar.
            - Evita adicionar URLs duplicados.
            - Replica o pedido para as réplicas dos barrels disponíveis.
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
        Método takeNext:
            - Retira o próximo URL da fila de URLs a visitar.
            - Marca o URL como visitado.
            - Notifica as réplicas dos barrels para também retirarem o próximo URL.
            - Retorna o URL retirado.
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
                        stub.takeProcessedUrl(empty_pb2.Empty()) # notifica a réplica para retirar o  URL
                        print(f"[Replica] Sucesso em takeNext para réplica (tentativa {attempt + 1}).")
                        break
                    except grpc.RpcError as e:
                        print(f"[Replica] Falha ao replicar takeNext para réplica (tentativa {attempt + 1})")

            return index_pb2.TakeNextResponse(url=url)

        except Exception as e:
            print(f"Erro ao retirar URL da fila: {e}")
            return index_pb2.TakeNextResponse(url="")
        
    def takeProcessedUrl(self, request, context):
        """
        Método takeProcessedUrl:
            - Retira o próximo URL da fila de URLs a visitar.
        """

        for url in self.fila:
            if url == request.url:
                self.fila.remove(url)
                print(f"[Fila] URL processado removido: {url}. Restam {self.fila.qsize()} URLs.")
                break
        
    def getIndexSize(self, request, context):
        """
        Método getIndexSize:
            - Retorna o tamanho do índice invertido.
        """
        return index_pb2.BarrelSizeResponse(size=len(self.indice_invertido))

def server(port, barrel_id):
    """
    Função server:
        - Inicia o servidor gRPC do Barrel.
        - Configura as opções de tamanho máximo de mensagem.
        - Carrega a configuração para obter informações sobre réplicas.
        - Regista os serviços do Barrel no servidor gRPC.
        - Inicia o servidor e aguarda por terminação.
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
    Função load_config:
        - Carrega a configuração do ficheiro JSON especificado.
        - Retorna o conteúdo do ficheiro como um dicionário Python.
    """
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERRO: Ficheiro de configuração '{file_path}' não encontrado.")
        sys.exit(1)
    
if __name__ == '__main__':
    """
    Main:
        - Verifica o ID do barrel passado como argumento.
        - Carrega a configuração do ficheiro JSON.
        - Procura a porta correspondente ao ID do barrel.
        - Inicia o servidor gRPC do Barrel.
    """

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