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
       

    def search(self):
        text = input("Insira pesquisa: ")

        try:
            response = self.stub.searchWord(index_pb2.SearchWordRequest(word=text))
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
        print("\n--- Estatísticas do Sistema ---")
        try:
            # Chama o novo método RPC na Gateway
            response = self.stub.getStatistics(empty_pb2.Empty()) 

            # 1. Top 10 pesquisas
            print("\nTop 10 Pesquisas Mais Comuns:")
            if response.top_searches:
                for i, word in enumerate(response.top_searches):
                    print(f"   {i+1}. {word}")
            else:
                print("   Nenhuma pesquisa registada.")


            # 2. Estatísticas dos Barrels (Tamanho e Tempo Médio)
            print("\n Estatísticas:")
            if response.barrel_stats:
                print(f"{'ID do Barrel'} | {'Tamanho do Índice'} | {'Tempo Médio (ds)'}")
                for stat in response.barrel_stats:
                    # Se o ID for o endereço, só mostrar a porta para simplificar
                    display_id = stat.barrel_id.split(':')[-1] if ':' in stat.barrel_id else stat.barrel_id
                    print(f"{'Porta ' + display_id} | {stat.index_size} | {stat.avg_response_time}")
            else:
                print("   Nenhuma estatística de Barrel disponível.")

        except grpc.RpcError as e:
            print(f"Erro ao obter estatísticas da Gateway: {e.details()}")

    def search_backlinks(self, url):
        """
        Método que obtém os backlinks de um URL.
        Apresenta a lista de URLs que apontam para o URL pedido.
        """
        try:
            response = self.stub.getBacklinks(index_pb2.PutNewRequest(url=url))
            backlinks = response.backlinks

            if not backlinks:
                print(f"\nNenhum backlink encontrado para '{url}'.")
                return

            print(f"\nBacklinks para '{url}':")
            for backlink in backlinks:
                #print(f"- {backlink.url}")
                print(f"- {backlink}")

        except grpc.RpcError as e:
            print(f"Erro ao obter backlinks: {e}")


    def menu(self):
       """
       Menu principal do cliente. Este vai apresentar todas as opções disponíveis ao cliente.
       """

       while True:
            print("\n--- Googol ---")
            print("1. Adicionar URL manualmente")
            print("2. Pesquisar palavra(s)")
            print("3. Ver backlinks de um URL")
            print("4. Estatísticas do sistema")
            print("5. Sair")

            response = input("Escolha uma das opções (1-5):")

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
                    url = input("Insire o URL para ver os backlinks: ")
                    if not url.startswith("http://") and not url.startswith("https://"):
                        print("URL inválido. Deve começar com 'http://' ou 'https://'.")
                        continue
                    self.search_backlinks(url)
                    
                
                elif response == "4":
                    self.statistics()
                
                elif response == "5":
                    print("A sair...")
                    break
                else:
                    print("Opção inválida. Por favor, escolha uma opção entre 1 e 5.")

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