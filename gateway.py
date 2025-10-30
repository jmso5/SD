import grpc
import index_pb2
import index_pb2_grpc
from google.protobuf import empty_pb2
import queue    
import json
import random
from concurrent import futures

MAX_MESSAGE_LENGTH = 262144000
MAX_RETRIES = 5

class Gateway(index_pb2_grpc.GatewayServicer, index_pb2_grpc.StorageBarrelServicer):

    def __init__(self, barrel_addresses, downloader_addresses):
        self.barrel_addresses = barrel_addresses
        self.stubs = []
        self.downloader_stubs = []

        options = [
            ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
            ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
        ]

        for address in barrel_addresses:
            stub = index_pb2_grpc.StorageBarrelStub(grpc.insecure_channel(address, options=options))
            self.stubs.append(stub)

        for address in downloader_addresses:
            stub = index_pb2_grpc.DownloaderStub(grpc.insecure_channel(address, options=options))
            self.downloader_stubs.append(stub)

        self.cache = {} # Cache local para palavras já pesquisadas evitando chamadas repetidas ao Barrel
        self.most_common_searches = {} # Dicionário para contar as pesquisas mais comuns, chave: palavra, valor: número de pesquisas


    def searchWord(self, request, context, max_tries=5):
        """
        Método searchWord:
            Recebe um pedido de um cliente para pesquisar uma palavra.
            Verifica primeiramente se essa palavra já foi pesquisada anteriormente, ou seja, se já se encontra na cache local.
            Caso não esteja na cache, vai pesquisar a palabra em cada Barrel até obter uma resposta.
            Se não obtiver nenhuma resposta tenta novamente até um máximo de "max_tries".
            Atualiza o cache local com os resultados obtidos.
        """
        self.most_common_searches[request.word] = self.most_common_searches.get(request.word, 0) + 1

        #Verifica se a palavra já está na cache
        if request.word in self.cache:
            results = self.cache[request.word]
            return index_pb2.SearchWordResponse(results=results)
        
        # Palavra não está na cache, pesquisa nos Barrels
        else:
            for attempt in range(max_tries):
                stub = random.choice(self.stubs) # Escolhe um Barrel aleatoriamente
                try:
                    response = stub.searchWord(request)
                    if response.results:
                        self.cache[request.word] = response.results
                        #return response
                        return index_pb2.SearchWordResponse(results=response.results)
                except grpc.RpcError as e:
                    print(f"Erro ao contactar o Barrel: {e}")
            return index_pb2.SearchWordResponse(results=[]) # Retorna resultados vazios se não encontrar nada após as tentativas
      


    def searchVariousWords(self, request, context, max_retries=5):
        """
        Método searchVariousWords:
            Recebe um pedido de um cliente para pesquisar várias palavras.
            Tenta pesquisar as palavras em cada Barrel até obter uma resposta.
            Se não obtiver nenhuma resposta tenta novamente até um máximo de "max_retries".
            Retorna os resultados obtidos.
        """
        for word in request.word:
            self.most_common_words[word] = self.most_common_words.get(word, 0) + 1
        attempt = 0
        while attempt < max_retries:
            stub = self.get_barrel_stub()
            try:
                response = stub.searchVariousWords(index_pb2_grpc.SearchVariousWordsRequest(words=request.word))
                break
            except grpc.RpcError as e:
                print(f"RPC failed: {e}")
                print(f"RPC error details: {e}")
                attempt += 1
                continue
        return response
    

    def getBacklinks(self, request, context):
        """
        Método que obtém os backlinks de um URL.
        Apresenta a lista de URLs que apontam para o URL pedido.
        """
        url = request.url
        
        for attempt in range(MAX_RETRIES):
            try:
                stub = random.choice(self.downloader_stubs) # Escolhe um Downloader aleatoriamente

                response = stub.getBacklinks(index_pb2.PutNewRequest(url=url))
                backlinks = response.backlinks

                if backlinks:
                    return backlinks

            except grpc.RpcError as e:
                print(f"[Gateway] Erro ao contactar Barrel em getBacklinks: {e}")

        return []



    def putNew(self, request, context, max_retries=5):
        """
        Método que recebe um pedido do cliente para adicionar um novo URL à fila de URLs a visitar,
        encaminhando o pedido para um Barrel aleatório com retry.
        """

        for attempt in range(max_retries):
            stub = random.choice(self.stubs) # Escolhe um Barrel aleatoriamente
            
            try:
                # Encaminha o pedido PutNew para o Barrel
                response = stub.putNew(request) 
                print(f"[Gateway] Encaminhado putNew para Barrel: {request.url}")
                return response 
            except grpc.RpcError as e:
                # A tentativa falhou, continua para a próxima tentativa ou sai
                print(f"[Gateway] Erro ao contactar Barrel em putNew: {e}")

        # Se falhar após todas as tentativas
        return empty_pb2.Empty()
    
    def takeNext(self, request, context, max_retries=5):
        """
        Verifica se tem URLs na fila de URLs a visitar, solicitando a um Barrel aleatório com retry.
        """
        for attempt in range(max_retries):
            # Escolhe um Barrel aleatoriamente
            stub = random.choice(self.stubs)
            
            try:
                response = stub.takeNext(request) 

                # Se o Barrel retornar um URL válido, retorna imediatamente
                if response.url:
                    return response
                
                # Se o Barrel retornar URL vazio, tenta outro Barrel random
            except grpc.RpcError as e:
                print(f"[Gateway] Erro ao contactar Barrel em takeNext: {e}")

        # Se todos os Barrels estiverem indisponíveis ou retornarem fila vazia após as tentativas
        return index_pb2.TakeNextResponse(url="") # Retorna URL vazio
        
def load_config():
    with open('config.json', 'r') as f:
        config = json.load(f)
    return config

if __name__ == "__main__":

    config = load_config()
    barrel_addresses = []
    downloader_addresses = []

    for addr in config['barrels']:
        barrel_addresses.append(f"{addr['ip']}:{addr['port']}")


    for addr in config['downloaders']:
        downloader_addresses.append(f"{addr['ip']}:{addr['port']}")

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
            ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
        ]
    )

    #server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    gateway = Gateway(barrel_addresses, downloader_addresses)
    index_pb2_grpc.add_GatewayServicer_to_server(gateway, server) # adiciona serviço Gateway
    index_pb2_grpc.add_StorageBarrelServicer_to_server(gateway, server) # adiciona serviço StorageBarrel


    porta = config['gateway']['port']
    server.add_insecure_port(f"[::]:{porta}")
    server.start()
    print(f"Gateway iniciado na porta {porta}")
    server.wait_for_termination()