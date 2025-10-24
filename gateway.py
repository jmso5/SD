import grpc
import index_pb2
import index_pb2_grpc
from google.protobuf import empty_pb2
import queue    
import json
import random
from concurrent import futures


class Gateway(index_pb2_grpc.GatewayServicer, index_pb2_grpc.StorageBarrelServicer):

    def __init__(self, barrel_addresses, queue_address='localhost:8284'):
       
        self.barrel_addresses = barrel_addresses
        self.stubs = []
        for address in barrel_addresses:
            stub = index_pb2_grpc.StorageBarrelStub(grpc.insecure_channel(address))
            self.stubs.append(stub)

        #self.queue_channel = grpc.insecure_channel(queue_address)
        #self.queue_stub = index_pb2_grpc.QueueStub(self.queue_channel)

        self.urls_a_visitar = queue.Queue()
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
                        return index_pb2.SearchWordResponse(results=results)
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



    def putNew(self, request, context):
        """
        Método que recebe um pedido do cliente para adicionar um novo URL à fila de URLs a visitar.
        """

        self.urls_a_visitar.put(request.url)
        return empty_pb2.Empty()
    
    def takeNext(self, request, context):
        """
        Verifica se tem URLs na fila de URLs a visitar.
        Retorna o próximo URL ou uma string vazia se a fila estiver vazia.
        Usa o método get_nowait() para evitar bloqueios dos downloaders no acesso à queue.
        """

        try:
            url = self.urls_a_visitar.get_nowait()
            return index_pb2.TakeNextResponse(url=url)
        except queue.Empty:
            return index_pb2.TakeNextResponse(url="")
        
def load_config():
    with open('config.json', 'r') as f:
        config = json.load(f)
    return config

if __name__ == "__main__":
    '''config = load_config()
    barrel_addresses = []
    for barrel in config['barrels']:
        address = f"{barrel['ip']}:{barrel['port']}"
        barrel_addresses.append(address)

    gateway = Gateway(config['barrels'])'''
    config = load_config()
    barrel_addresses = [f"{b['ip']}:{b['port']}" for b in config['barrels']]

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    gateway = Gateway(barrel_addresses)
    index_pb2_grpc.add_GatewayServicer_to_server(gateway, server)

    index_pb2_grpc.add_GatewayServicer_to_server(gateway, server) # adiciona serviço Gateway
    index_pb2_grpc.add_StorageBarrelServicer_to_server(gateway, server) # adiciona serviço StorageBarrel

    porta = config['gateway']['port']
    server.add_insecure_port(f"[::]:{porta}")
    server.start()
    print(f"Gateway iniciado na porta {porta}")
    server.wait_for_termination()