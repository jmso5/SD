from concurrent import futures
import grpc
from google.protobuf import empty_pb2
import index_pb2
import index_pb2_grpc

class IndexServicer(index_pb2_grpc.IndexServicer):
    def __init__(self):
        self.indice_invertido = {}

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


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    index_pb2_grpc.add_IndexServicer_to_server(IndexServicer(), server)
    server.add_insecure_port('[::]:8183')
    server.start()
    print("Index Storage Barrel iniciado na porta 8183")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("A encerrar o Barrel...")
        server.stop(0)


if __name__ == '__main__':
    serve()
