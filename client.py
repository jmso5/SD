import grpc
import index_pb2_grpc
import index_pb2
from google.protobuf import empty_pb2
import json
import sys
import time

MAX_MESSAGE_LENGTH = 262144000 # tamanho máximo de mensagem

class Client:
   
    def __init__(self, sever_ip, server_port):
        # permite enviar e receber mensagens grandes
        options = [
            ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
            ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
        ]

        self.channel = grpc.insecure_channel(f"{sever_ip}:{server_port}", options=options) # cria canal gRPC com o gateway
        self.stub = index_pb2_grpc.GatewayStub(self.channel) # cria stub (cliente) -> putNew / takeNext
       

    def search(self):
        """
        Método que pesquisa de palavras.
        """

        text = input("Insira pesquisa: ")
        words = text.lower().split()

        if not words:
            print("Pesquisa inválida.")
            return
        
        print(f"A pesquisar por {' '.join(words)}...")

        try:
            # 1. Escolhe o RPC: searchWord (1 palavra) ou searchVariousWords (>1 palavra)
            if len(words) == 1:
                # Chama o RPC searchWord para uma palavra
                request = index_pb2.SearchWordRequest(word=words[0])
                response = self.stub.searchWord(request)
            else:
                # Chama o RPC searchVariousWords para várias palavras
                request = index_pb2.SearchVariousWordsRequest(words=words)
                response = self.stub.searchVariousWords(request)

            results = response.results  # lista de resultados da pesquisa

            if not results:
                print(f"\nNenhum resultado para '{text}'.")
                return

            # mostra resultados paginados de 10 em 10
            for i in range(0, len(results), 10):
                grupo = results[i:i+10]
                print(f"\n--- Página {i//10 + 1} ---")
                for r in grupo:
                    print(f"\nTÍTULO -> {r.title}")
                    print(f"URL -> {r.url}")
                    print(f"CITAÇÃO -> {r.text[:200]}")  
                    print("-" * 60)

                # se houver mais resultados, pergunta se quer continuar
                if i + 10 < len(results):
                    continuar = input("\nMostrar mais resultados? (Enter para continuar, 'n' para parar): ")
                    if continuar.lower() == 'n':
                        break

        except grpc.RpcError as e:
            print(f"Não foi possível realizar o seu pedido. Tente novamente em 5 segundos.")
            time.sleep(5)

    def statistics(self):
        """
        Método que obtém as estatísticas do sistema.
        Apresenta as 10 pesquisas mais comuns, a lista de barrels ativos e os respetivos tamanhos dos indices 
        e o tempo médio de resposta a pesquisas medido pela Gateway descriminado por Barrel (em décimas de segundo).
        """

        print("\n--- Estatísticas do Sistema ---")
        try:
            response = self.stub.getStatistics(empty_pb2.Empty()) # chama o método RPC getStatistics do gateway

            # 1. top 10 pesquisas
            print("\nTop 10 Pesquisas Mais Comuns:")
            if response.top_searches:
                for i, word in enumerate(response.top_searches):
                    print(f"   {i+1}. {word}")
            else:
                print("\tNenhuma pesquisa registada.")


            # 2. estatísticas dos Barrels (tamanho do índice + tempo médio de resposta)
            print("\n Estatísticas:")
            if response.barrel_stats:
                print(f"{'ID do Barrel'} | {'Tamanho do Índice'} | {'Tempo Médio (ds)'}")
                for stat in response.barrel_stats:
                    #  mostra apenas a porta (não o IP completo) para simplificar a visualização
                    if ':' in stat.barrel_id:
                        display_id = stat.barrel_id.split(':')[-1]
                    else:
                        display_id = stat.barrel_id

                    print(f"{'Porta ' + display_id} | {stat.index_size} | {stat.avg_response_time}")
            else:
                print("Nenhuma estatística de Barrel disponível.")

        except grpc.RpcError as e:
            print(f"Não foi possível realizar o seu pedido. Tente novamente em 5 segundos.")
            time.sleep(5)

    def search_backlinks(self, url):
        """
        Método que obtém os backlinks de um URL.
        Apresenta a lista de URLs que apontam para o URL pedido.
        """

        
        try:
            response = self.stub.getBacklinks(index_pb2.PutNewRequest(url=url)) # chama o método RPC getBacklinks do gateway
            backlinks = response.backlinks # lista de urls que apontam para o url pedido

            if not backlinks:
                print(f"\nNenhum backlink encontrado para '{url}'.")
                return

            print(f"\nBacklinks para '{url}':")
            for backlink in backlinks:
                print(f"- {backlink}")

        except grpc.RpcError as e:
            print(f"Não foi possível realizar o seu pedido. Tente novamente em 5 segundos.")
            time.spleep(5)


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
                # Adicionar URL manualmente
                if response == "1":
                    url = input("Insire o URL: ")
                    if not url.startswith("http://") and not url.startswith("https://"): # verifica se começa por http:// ou https://
                        print("URL inválido. Deve começar com 'http://' ou 'https://'.")
                        continue

                    # envia o URL para o gateway -> vai para a fila de URLs por visitar
                    request = index_pb2.PutNewRequest(url=url)
                    self.stub.putNew(request)

                # Pesquisar palavras
                elif response == "2":
                    self.search()

                # Ver backlinks de um URL
                elif response == "3":
                    url = input("Insire o URL para ver os backlinks: ")
                    if not url.startswith("http://") and not url.startswith("https://"):
                        print("URL inválido. Deve começar com 'http://' ou 'https://'.")
                        continue
                    self.search_backlinks(url)

                # Estatísticas do sistema
                elif response == "4":
                    self.statistics()
                
                elif response == "5":
                    print("A sair...")
                    break
                else:
                    print("Opção inválida. Por favor, escolha uma opção entre 1 e 5.")

            except grpc.RpcError as e:
                print(f"Não foi possível realizar o seu pedido. Tente novamente em 5 segundos.")
                time.sleep(5)

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

if __name__ == "__main__":

    config = load_config('config.json') # carrega IP e porta do Gateway a partir do config.json
    client = Client(config['gateway']['ip'], config['gateway']['port']) # cria instância do cliente

    # não dar erro em caso de ^C
    try:
        client.menu()
    except KeyboardInterrupt:
        print("A encerrar o Cliente...")