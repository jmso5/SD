import grpc
import index_pb2
import index_pb2_grpc
from google.protobuf import empty_pb2


class Client:
   
    def __init__(self, sever_ip, server_port):
        self.channel = grpc.insecure_channel(f"{sever_ip}:{server_port}") # cria canal gRPC com o gateway
        self.stub = index_pb2_grpc.IndexStub(self.channel) # cria stub (cliente)


    def menu(self):
       """
       Menu principal do cliente. Este vai apresentar todas as opções disponíveis ao cliente.
       """

       while True:
            print("--- Googol ---")
            print("1. Adicionar URL manualmente")
            print("2. Pesquisar palavra(s)")
            print("3. Estatísticas do sistema")
            print("4. Sair")

            response = input("Escolha uma das opções (1-4):")

            try:
                if response == "1":
                    url = input("Insire o URL: ")
                    if not url.startswith("http://") and not url.startswith("https://"):
                        print("URL inválido. Deve começar com 'http://' ou 'https://'.")
                        continue

                    # Chama o método remoto para adicionar o URL
                    request = index_pb2.PutNewRequest(url=url)
                    self.stub.putNew(request)

                elif response == "2":
                    palavra = input("Insira a palavra(s) que pretende pesquisar: ")
                
                elif response == "3":
                    print("Estatísticas do sistema ainda não implementadas.")

            except grpc.RpcError as e:
                print(f"Erro ao processar URL {url}: {e}")
