"""
    Módulo Gateway: Servidor Central de Comunicação

   - Este módulo implementa a classe Gateway, que atua como servidor (server gRPC) para
   gerenciar a comunicação com os clientes, Barrels e Downloaders.

   O Gateway é responsável por:
    1. Receber pedidos de pesquisa de palavras dos clientes e encaminhar esses pedidos para os Barrels disponíveis.
    3. Balanceamento de carga e lógica de retry para Barrels (Failover).
    4. Receber pedidos de backlinks dos clientes e encaminhá-los para os Downloaders.
    5. Gerenciar a fila de URLs a visitar (putNew e takeNext) encaminhando os pedidos para os Barrels.
    6. Coletar e fornecer estatísticas sobre pesquisas e desempenho dos Barrels.  

    Para estas funcionalidades temos os seguintes métodos:
        - searchWord: Pesquisa uma única palavra e retry.
        - searchVariousWords: Pesquisa várias palavras, agregando resultados.
        - getBacklinks: Obtém backlinks de um URL, agregando resultados de vários Downloaders.
        - putNew: Adiciona um novo URL à fila de URLs a visitar.
        - takeNext: Obtém o próximo URL da fila de URLs a visitar.
        - getStatistics: Fornece estatísticas sobre pesquisas e desempenho dos Barrels.
"""


import grpc
import index_pb2
import index_pb2_grpc
from google.protobuf import empty_pb2  
import json
import random
from concurrent import futures
import time
import sys
import pickle
import os

# constantes globais
MAX_MESSAGE_LENGTH = 262144000 # tamanho máximo de mensagem
MAX_RETRIES = 5 # número máximo de tentativas 

class Gateway(index_pb2_grpc.GatewayServicer, index_pb2_grpc.StorageBarrelServicer):
    """
    - O Gateway mantém a lista dos Barrels e Downloaders ativos, gerencia a comunicação com eles
    e fornece uma interface de acesso aos Clientes.


    - Atributos:
        - barrel_addresses: Lista de endereços dos Barrels.
        - downloader_addresses: Lista de endereços dos Downloaders.
        - barrel_stubs: Lista de stubs gRPC para os Barrels.
        - barrel_stubs_map: Mapeia endereços para stubs de Barrels.
        - downloader_stubs: Lista de stubs gRPC para os Downloaders.
        - most_common_searches: Dicionário para contar as pesquisas mais comuns.
        - response_times: Guarda tempos de resposta por Barrel.
        - statistics_file: Ficheiro para guardar estatísticas.
    """

    def __init__(self, barrel_addresses, downloader_addresses, statistics_file='gateway_statistics.pkl'):
        """
        - Inicializa o Gateway, criando os canais gRPC e os stubs necessários para se conectar com os Barrels e Downloaders.
        - Inicia as variáveis endereços e estatísticas e carrega estatísticas anteriores a partir de um ficheiro.
        """

        # guarda endereços de Barrels e Downloaders
        self.barrel_addresses = barrel_addresses
        self.downloader_addresses = downloader_addresses

        self.downloader_stubs = [] # lista de stubs para os Downloaders
        self.barrel_stubs = []
        self.barrel_stubs_map = {} # mapeia endereços para stubs de Barrels 
        self.downloader_stubs_map = {}



        # opções de limites de tamanho de mensagem
        options = [
            ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
            ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
        ]

        self.response_times = {} # guarda tempos de resposta por Barrel

        for addr in barrel_addresses:
            self.response_times[addr] = [] # cada barrel começa com lista vazia

        # conecta aos Barrels ativos
        for address in barrel_addresses:
            stub = index_pb2_grpc.StorageBarrelStub(grpc.insecure_channel(address, options=options))
            self.barrel_stubs.append(stub) # adiciona à lista de stubs de Barrels para usar 
            self.barrel_stubs_map[stub] = address # mapeia endereço para stub
        
    
        # conecta aos Downloaders 
        for address in downloader_addresses:
            stub = index_pb2_grpc.DownloaderStub(grpc.insecure_channel(address, options=options))
            self.downloader_stubs.append(stub)
            self.downloader_stubs_map[stub] = address

        self.most_common_searches = {} # dicionário para contar as pesquisas mais comuns

        self.statistics_file = statistics_file
        self.load_statistics()

    def available_barrels(self):
        """
        Método available_barrels:
            - Verifica quais os Barrels disponíveis (ativos) através de chamadas de health check.
            - Retorna uma lista de stubs dos Barrels disponíveis.
        """
        available_barrels = []        
        for stub in self.barrel_stubs:
            try:
                stub.checkHealth(empty_pb2.Empty()) 
                available_barrels.append(stub)
            except grpc.RpcError:
                continue

        return available_barrels
    
    def available_downloaders(self):
        """
        Método available_downloaders:
            - Verifica quais os Downloaders disponíveis (ativos) através de chamadas de health check.
            - Retorna uma lista de stubs dos Downloaders disponíveis.
        """
        available_downloaders = []        
        for stub in self.downloader_stubs:
            try:
                stub.checkHealth(empty_pb2.Empty()) 
                available_downloaders.append(stub)
            except grpc.RpcError:
                continue

        return available_downloaders
    
    def load_statistics(self):
        """
        Método load_statistics:
            - Carrega as estatísticas de pesquisas a partir de um ficheiro.
        """

        if os.path.exists(self.statistics_file):
            try:
                with open(self.statistics_file, 'rb') as f:
                    self.most_common_searches = pickle.load(f)
                print("[Gateway] Estatísticas carregadas com sucesso.")
            except Exception as e:
                print(f"[Gateway] Erro ao carregar estatísticas: {e}")
        else:
            print("[Gateway] Nenhum ficheiro de estatísticas encontrado. Iniciando com estatísticas vazias.")

    def save_statistics(self):
        """
        Método save_statistics:
            - Guarda as estatísticas de pesquisas num ficheiro.
        """

        try:
            with open(self.statistics_file, 'wb') as f:
                pickle.dump(self.most_common_searches, f)
            print("[Gateway] Estatísticas guardadas com sucesso.")
        except Exception as e:
            print(f"[Gateway] Erro ao guardar estatísticas: {e}")

    def searchWord(self, request, context, max_tries=5):
            """
        Método searchWord:
            - Recebe um pedido de um cliente para pesquisar uma palavra.
            - Se não obtiver nenhuma resposta tenta novamente até um máximo de "max_tries".
            - Calcula o tempo de resposta do barrel e atualiza as respetivas estatísticas.
        """
       
            word = request.word
            self.most_common_searches[word] = self.most_common_searches.get(word, 0) + 1 # contagem da palavra nas estatisticas

            available_barrels = self.available_barrels()

            # Verifica se há Barrels disponíveis
            if not available_barrels:
                print("Nenhum Barrel disponível para pesquisa.")
                return index_pb2.SearchWordResponse(results=[])
    
            # seleção de Barrel aleatório e obtém stub
            stub = random.choice(available_barrels)
            barrel_address = self.barrel_stubs_map[stub] # obtém endereço do barrel selecionado para as estatísticas

            start_time = time.time() # ínicio do tempo de resposta

            for attempt in range(max_tries):
                try:
                    response = stub.searchWord(request) # chamada RPC ao Barrel
                    end_time = time.time()
                    
                    # calcula tempo em décimas de segundo e guarda
                    response_time_ds = (end_time - start_time) * 10
                    self.response_times[barrel_address].append(response_time_ds) 

                    if response.results:
                        print(f"Palavra '{word}' encontrada no Barrel {barrel_address} com {len(response.results)} resultados.")
                        return index_pb2.SearchWordResponse(results=response.results)
                except grpc.RpcError as e:
                    print(f"Erro ao contactar o Barrel: {e}")
            return index_pb2.SearchWordResponse(results=[]) # Retorna resultados vazios se não encontrar nada após as tentativas

   
    def searchVariousWords(self, request, context):
        """
        Método searchVariousWords:
            - Recebe um pedido de um cliente para pesquisar várias palavras.
            - Tenta pesquisar as palavras num Barrel "random" um máximo de "MAX_RETIRES" até obter uma resposta.
            - Calcula o tempo de resposta do barrel e atualiza as respetivas estatísticas.
            - Agrega os resultados obtidos, removendo duplicados e ordenando pela relevância (links_count).
            - Retorna os resultados obtidos.
        """

        all_results = {} # Usar dicionário para armazenar resultados por URL e evitar duplicados
        available_barrels = self.available_barrels()

        if not available_barrels:
            print("[Gateway] Nenhum Barrel disponível para pesquisa.")
            return index_pb2.SearchWordResponse(results=[])
        
        # atualiza estatísticas de pesquisas
        for word in request.words: 
            self.most_common_searches[word] = self.most_common_searches.get(word, 0) + 1



        start_time = time.time() # ínicio do tempo de resposta

        for attempt in range(MAX_RETRIES):
            try:
                stub = random.choice(available_barrels)
                barrel_address = self.barrel_stubs_map[stub]

                request_barrel = index_pb2.SearchVariousWordsRequest(words=request.words)
                response = stub.searchVariousWords(request_barrel) # Chama o searchWord no Barrel
                end_time = time.time()

                total_time = (end_time - start_time) * 10 # acumula tempo em décimas de segundo

                self.response_times[barrel_address].append(total_time)
                    
                # Agrega os resultados
                for result in response.results:
                    if result.url not in all_results:
                         all_results[result.url] = result # Armazena o objeto SearchResult
                    
                break # Sai do loop de tentativas se o Barrel responder
            except grpc.RpcError as e:
                print(f"[Gateway] Erro ao contactar Barrel (tentativa {attempt + 1}): {e}")
            
        self.save_statistics() # Guarda as estatísticas após processar todas as palavras
        

        # Converte os resultados do dicionário de volta para uma lista de SearchResult
        final_results_list = list(all_results.values())
        
        # Ordenar os resultados agregados pela relevância (links_count)
        final_results_list.sort(key=lambda x: x.links_count, reverse=True)
        
        print(f"[Gateway] Pesquisa em várias palavras resultou em {len(final_results_list)} resultados únicos.")
        return index_pb2.SearchWordResponse(results=final_results_list)
    

    def getBacklinks(self, request, context, max_retries=5):
        """
        Método getBacklinks:
            - Recebe um pedido do cliente para obter backlinks de um URL.
            - Encaminha o pedido para todos os Downloaders ativos, agregando os resultados.
            - Implementa lógica de retry para cada Downloader.
            - Retorna todos os backlinks encontrados.
        """
        url = request.url
        all_backlinks = set()  # conjunto para evitar duplicados
        
        if not self.downloader_stubs:
            print("[Gateway] Nenhum Downloader ativo.")
            return index_pb2.BacklinksResponse(backlinks=[])
            
        # itera sobre cada Downloader ativo
        available_downloaders = self.available_downloaders()

                
                
        for attempt in range(max_retries):
            stub = random.choice(available_downloaders)
                
                
            address = self.downloader_stubs_map.get(stub)
            try:
                response = stub.getBacklinks(index_pb2.PutNewRequest(url=url)) # envia o pedido para este Downloader

                if response.backlinks:
                    all_backlinks.update(response.backlinks) # Adiciona todos os backlinks encontrados
                        
                print(f"[Gateway] Downloader em {address} contribuiu com {len(response.backlinks)} backlinks.")
                break
                        
            except grpc.RpcError as e:
                print(f"[Gateway] Erro ao contactar Downloader em {address} (tentativa {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    print(f"[Gateway] Falha permanente ao contactar Downloader em {address}.")
            

        # retornar todos os resultados agregados
        return index_pb2.BacklinksResponse(backlinks=list(all_backlinks))


    def putNew(self, request, context):
        """
        Método putNew:
            - Recebe um pedido do cliente para adicionar um novo URL à fila de URLs a visitar.
            - Encaminha o pedido para um Barrel aleatório, implementando lógica de retry.
        """

        available_barrels = self.available_barrels()    

        for attempt in range(MAX_RETRIES):
            stub = random.choice(available_barrels) # escolhe um Barrel aleatoriamente
            
            try:
                # encaminha o pedido PutNew para o Barrel
                response = stub.putNew(request) 
                print(f"[Gateway] Encaminhado putNew para Barrel: {request.url}")
                return response 
            except grpc.RpcError as e:
                # tentativa falhou -> continua para a próxima tentativa ou sai
                print(f"[Gateway] Erro ao contactar Barrel em putNew: {e}")

        return empty_pb2.Empty() # falha após todas as tentativas
    
    def takeNext(self, request, context, max_retries=5):
        """
        Método takeNext:
            - Recebe um pedido do cliente para obter o próximo URL da fila de URLs a visitar.
            - Encaminha o pedido para um Barrel aleatório, implementando lógica de retry.
            - Retorna o URL obtido ou vazio se nenhum Barrel retornar um URL válido.
        """

        available_barrels = self.available_barrels()

        if not available_barrels:
            print("[Gateway] Nenhum Barrel disponível para takeNext.")
            return index_pb2.TakeNextResponse(url="")

        for attempt in range(max_retries):
            stub = random.choice(available_barrels) # escolhe um Barrel aleatoriamente

            try:
                response = stub.takeNext(request) 
    
                # se o Barrel retornar um URL válido -> retorna imediatamente
                if response.url:
                    return response
                
                # se o Barrel retornar URL vazio -> tenta outro Barrel random
            except grpc.RpcError as e:
                print(f"[Gateway] Erro ao contactar Barrel em takeNext: {e}")

        # nenhum Barrel retornou um URL válido
        return index_pb2.TakeNextResponse(url="") # Retorna URL vazio
    
    def getStatistics(self, request, context):
        """
        Método getStatistics:
            - Recebe um pedido do cliente para obter estatísticas do sistema.
            - Compila as 10 pesquisas mais comuns e estatísticas de cada Barrel (tamanho do índice e tempo médio de resposta).
            - Retorna as estatísticas compiladas.
        """

        # 1. Obter as 10 pesquisas mais comuns
        top_searches_list = sorted(
            self.most_common_searches.items(),
            key=lambda item: item[1],
            reverse=True
        )[:10] 
        
        top_searches_words = [word for word, count in top_searches_list]

        # 2. Obter estatísticas dos Barrels (tamanho e tempo médio)
        barrel_stats_list = []

        available_barrels = self.available_barrels()

        for stub in available_barrels:
            
            barrel_address = self.barrel_stubs_map[stub]
            # tenta obter o tamanho do índice
            index_size = 0
            try:
                # chama o novo RPC no Barrel
                size_response = stub.getIndexSize(empty_pb2.Empty()) 
                index_size = size_response.size
            except grpc.RpcError as e:
                print(f"[Gateway Stats] Barrel em {barrel_address} indisponível ou erro: {e.details()}")
            
            # calcula o tempo médio de resposta
            times = self.response_times.get(barrel_address, [])
            avg_time = sum(times) / len(times) if times else 0.0 # em décimas de segundo

            for address in config['barrels']:
                addr_str = f"{address['ip']}:{address['port']}"
                if addr_str == barrel_address:
                    barrel_id = addr_str
                    break

            # cria objeto BarrelStatistic (para enviar ao cliente)
            barrel_stats_list.append(
                index_pb2.BarrelStatistic(
                    barrel_id=barrel_id, 
                    index_size=index_size,
                    avg_response_time=round(avg_time, 2)
                )
            )

        # devolve todas as estatísticas agregadas
        return index_pb2.StatisticsResponse(
            top_searches=top_searches_words,
            barrel_stats=barrel_stats_list
        )
    
    def addToIndex(self, request, context):
        
        available_barrels = self.available_barrels()

        if not available_barrels:
            print("[Gateway] Nenhum Barrel disponível para addToIndex.")
            return empty_pb2.Empty()
        for stub in available_barrels:
            for attempt in range(MAX_RETRIES):
                try:
                    stub.addToIndex(request) 
                    print(f"[Gateway] Encaminhado addToIndex para Barrel: {request.url}")
                    success = True # Marca que pelo menos um Barrel recebeu
                    break # Sai do loop de tentativas para o Barrel atual
                except grpc.RpcError as e:
                    print(f"[Gateway] Erro ao contactar Barrel em addToIndex: {e}.")
                    time.sleep(2)  
        
        # Retorna o Empty apenas depois de tentar todos os barrels
        if success:
            return empty_pb2.Empty()
        else:
            return empty_pb2.Empty() # Falhou em todos os barrels.
        
        
def load_config(file_path='config.json'):
    """
    Método load_config:
        - Carrega a configuração do sistema a partir de um ficheiro JSON.
        - Retorna o conteúdo do ficheiro como um dicionário.
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
        - Carrega a configuração do sistema.
        - Prepara listas de endereços dos Barrels e Downloaders.
        - Cria o servidor gRPC e regista os serviços do Gateway.
        - Inicia o servidor na porta configurada e aguarda por pedidos.
        - Trata encerramento do servidor, guardando as estatísticas num ficheiro pkl.
    """

    # carrega configuração do sistema
    config = load_config()
    barrel_addresses = []
    downloader_addresses = []

    # prepara listas de endereços dos barrels
    for addr in config['barrels']:
        barrel_addresses.append(f"{addr['ip']}:{addr['port']}")

    # prepara listas de endereços dos downloaders
    for addr in config['downloaders']:
        downloader_addresses.append(f"{addr['ip']}:{addr['port']}")

    # cria o servidor gRPC com até 10 threads
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
            ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
        ]
    )

    # instancia o gateway e regista os serviços no servidor
    gateway = Gateway(barrel_addresses, downloader_addresses)
    index_pb2_grpc.add_GatewayServicer_to_server(gateway, server) 
    index_pb2_grpc.add_StorageBarrelServicer_to_server(gateway, server) 

    # inicia o servidor na porta configurada
    porta = config['gateway']['port']
    server.add_insecure_port(f"[::]:{porta}")
    server.start()
    print(f"Gateway iniciado na porta {porta}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("A encerrar o Gateway...")
        gateway.save_statistics()
        server.stop(0)
