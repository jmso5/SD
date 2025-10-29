import grpc
import index_pb2_grpc
import index_pb2
from google.protobuf import empty_pb2
import json

MAX_MESSAGE_LENGTH = 262144000

class Client:
   
    def __init__(self, sever_ip, server_port):
        options = [
            ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
            ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
        ]

        self.channel = grpc.insecure_channel(f"{sever_ip}:{server_port}", options=options) # cria canal gRPC com o gateway
        self.stub = index_pb2_grpc.GatewayStub(self.channel) # cria stub (cliente) -> putNew / takeNext
        self.barrel_stub = index_pb2_grpc.StorageBarrelStub(self.channel) # addToIndex / searchWord

    def search(self):
        text = input("Insira pesquisa: ")

        try:
            response = self.barrel_stub.searchWord(index_pb2.SearchWordRequest(word=text))
            results = response.results

            if not results:
                print(f"\nNenhum resultado para '{text}'.")
                return

            for i in range(0, len(results), 10):
                grupo = results[i:i+10]
                print(f"\n--- Página {i//10 + 1} ---")
                for r in grupo:
                    print(f"\nTÍTULO -> {r.title}")
                    print(f"URL -> {r.url}")
                    print(f"CITAÇÃO -> {r.text[:200]}")  # primeiros 200 chars
                    print("-" * 60)

                if i + 10 < len(results):
                    continuar = input("\nMostrar mais resultados? (Enter para continuar, 'n' para parar): ")
                    if continuar.lower() == 'n':
                        break

        except grpc.RpcError as e:
            print(f"Erro ao pesquisar: {e}")

    def statistics(self):
        """
        Método que obtém as estatísticas do sistema.
        Apresenta as 10 pesquisas mais comuns, a lista de barrels ativos e os respetivos tamanhos dos indices 
        e o tempo médio de resposta a pesquisas medido pela Gateway descriminado por Barrel (em décimas de segundo).
        """


    def menu(self):
       """
       Menu principal do cliente. Este vai apresentar todas as opções disponíveis ao cliente.
       """

       while True:
            print("\n--- Googol ---")
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
                    self.search()
                    
                
                elif response == "3":
                    self.statistics()
                
                elif response == "4":
                    print("A sair...")
                    break
                else:
                    print("Opção inválida. Por favor, escolha uma opção entre 1 e 4.")

            except grpc.RpcError as e:
                print(f"Erro ao processar URL {url}: {e}")

def load_config(file_path):
    with(open(file_path, 'r')) as f:
        return json.load(f)

if __name__ == "__main__":
    #python client.py
    config = load_config('config.json')
    client = Client(config['gateway']['ip'], config['gateway']['port']) #inicia cliente com o ip e porta do gateway (definidos no config.json)
    client.menu()