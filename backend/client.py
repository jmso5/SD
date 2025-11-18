"""
    - Ficheiro client.py que será usado para o cliente poder comunicar com o sistema

    - O cliente comunica com o gateway através de gRPC para realizar s submissão de URLs manualmente, pesquisas de palavras,
    obtenção de backlinks e visualização de estatísticas do sistema. Dentra das estatísticas, serão apresentadas as 10 pesquisas mais comuns,
    a lista de barrels ativos e os respetivos tamanhos dos indices e o tempo médio de resposta a pesquisas nos Barrels medido pela Gateway (em décimas de segundo).

    - As configurações do cliente, como o IP e a porta do Gateway, são carregadas a partir de um ficheiro config.json.
"""

import grpc
import index_pb2_grpc
import index_pb2
from google.protobuf import empty_pb2
import json
import sys
import time

MAX_MESSAGE_LENGTH = 262144000 # tamanho máximo de mensagem

class Client:
    """
    -  Classe Client que representa a comunicação de um cliente com a Gateway que vai realizar os "requests" do cliente através do gRCP. Estes 
    requests vão ser posteriormente tratados pela Gateway e enviados para o resto do sistema. Recebe o IP e a porta do Gateway para estabelecer a comunicação.
    """
   
    def __init__(self, sever_ip, server_port):
        """
        - Inicia o cliente criando um canal gRPC e um stub para se conectar com o Gateway que é utilizada como servidor.

        - É definido um tamanho máximo para as mensagens enviadas e recebidas para permitir a transmissão de grandes quantidades de dados. 
        Evitando erros relacionados com o tamanho das mensagens.
        """

        # permite enviar e receber mensagens grandes
        options = [
            ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
            ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
        ]

        self.channel = grpc.insecure_channel(f"{sever_ip}:{server_port}", options=options) # cria canal gRPC com o gateway
        self.stub = index_pb2_grpc.GatewayStub(self.channel) # cria stub (cliente) -> putNew / takeNext

    def submit_url(self, url):
        """
        - Método que submete um URL para o sistema através do Gateway.
        - Envia um request putNew para o Gateway com o URL a ser indexado.
        """

        try:
            request = index_pb2.PutNewRequest(url=url)
            self.stub.putNew(request) # chama o método RPC putNew do gateway
            print(f"URL {url} adicionado para indexação.")
            
        except grpc.RpcError as e:
            print(f"Não foi possível realizar o seu pedido. Tente novamente em 5 segundos.")
            time.sleep(5)
       

    def search(self, query):
        """
        - Método que realiza a pesquisa de palavra(s) no sistema. É verificado se a pesquisa é de 1 ou mais palavras, sendo 
        chamada a função RPC adequada (searchWord ou searchVariousWords).

        - Os resultados são apresentados de forma paginada, 10 resultados por página, e o utilizador pode optar por ver mais resultados
        ou parar a visualização.

        - Em caso de erro na comunicação com o servidor, é apresentada uma mensagem de erro
        e o cliente espera 5 segundos antes de tentar novamente.
        """

        #text = input("Insira pesquisa: ")

        words = query.lower().split()

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
                
            print("AQUI")

            return response.results


            """
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
            """

        except grpc.RpcError as e:
            print(f"[Cliente] Não foi possível realizar o seu pedido. Tente novamente em 5 segundos.")
            time.sleep(5)

    def statistics(self):
        """
        - Método que obtém as estatísticas do sistema.
        - Apresenta as 10 pesquisas mais comuns, a lista de barrels ativos e os respetivos tamanhos dos indices 
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
        - Método que obtém os backlinks de um URL.
        - Apresenta a lista de URLs que apontam para o URL pedido.
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
            return backlinks

        except grpc.RpcError as e:
            print(f"Não foi possível realizar o seu pedido. Tente novamente em 5 segundos.")
            time.sleep(5)


    """

    def menu(self):
    
       - Menu principal do cliente que apresenta as opções disponíveis para o utilizador.
    

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
                    self.submit_url()

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

    """

def load_config(file_path='config.json'):
    """
    - Função para carregar as configurações dos ips e portas para o gateway a partir de um ficheiro JSON.
    """
    
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERRO: Ficheiro de configuração '{file_path}' não encontrado.")
        sys.exit(1)

if __name__ == "__main__":
    """
    - Ponto de entrada do programa. Carrega as configurações do ficheiro config.json, cria uma instância do cliente
    """

    config = load_config('config.json') # carrega IP e porta do Gateway a partir do config.json
    client = Client(config['gateway']['ip'], config['gateway']['port']) # cria instância do cliente

    # não dar erro em caso de ^C
    try:
        client.menu()
    except KeyboardInterrupt:
        print("A encerrar o Cliente...")
