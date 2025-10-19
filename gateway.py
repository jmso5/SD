import grpc
import index_pb2
import index_pb2_grpc
from google.protobuf import empty_pb2
import queue    
import json


class Gateway(index_pb2_grpc.Gateway):

    def _init_(self, barrel_address, queue_address='localhost:8284'):
        self.barrel_channel = grpc.insecure_channel(barrel_address)
        self.barrel_stub = index_pb2_grpc.IndexStub(self.barrel_channel)

        #self.queue_channel = grpc.insecure_channel(queue_address)
        #self.queue_stub = index_pb2_grpc.QueueStub(self.queue_channel)

        self.urls_a_visitar = queue.Queue()
        self.cache = {} # Cache local para palavras já pesquisadas evitando chamadas repetidas ao Barrel


    def searchWord(self, request, context, max_tries=5):
        """
        Método searchWord:
            Recebe um pedido de um cliente para pesquisar uma palavra.
            Verifica primeiramente se essa palavra já foi pesquisada anteriormente, ou seja, se já se encontra na cache local.
            Caso não esteja na cache, vai pesquisar a palabra em cada Barrel até obter uma resposta.
            Se não obtiver nenhuma resposta tenta novamente até um máximo de "max_tries".
            Atualiza o cache local com os resultados obtidos.
        """
      


    def searchVariousWords(self, request, context, max_retries=5):
        """
        Método searchVariousWords:
            Recebe um pedido de um cliente para pesquisar várias palavras.
            Tenta pesquisar as palavras em cada Barrel até obter uma resposta
            Se não obtiver nenhuma resposta tenta novamente até um máximo de "max_retries".
            Retorna os resultados obtidos.
        """

    def putNew(self, request, context):
        """
        Método que recebe um pedido do cliente para adicionar um novo URL à fila de URLs a visitar.
        """

        self.urls_a_visitar.put(request.url)
        return empty_pb2.Empty()
    
    def takeNext(self, request, context):
        """
        Método que recebe um pedido de um cliente para obter o próximo URL a visitar da fila.
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
    config = load_config()
    gateway = Gateway(config['barrels'])