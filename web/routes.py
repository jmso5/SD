"""
Este ficheiro guarda as rotas para a interface web do cliente.
Utiliza o framework FastAPI e o motor de templates Jinja2 para gerir o fluxo da aplicação, 
incluindo a pesquisa, indexação de URLs e monitorização do servidor.
"""
import json
import os
import sys
from fastapi import Form, FastAPI
from urllib.request import Request
import requests
from google.protobuf import empty_pb2 
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from fastapi.staticfiles import StaticFiles
from fastapi import Body, WebSocket, WebSocketDisconnect
from google import genai
import asyncio

# Adiciona a diretoria 'pai' (backend) ao sys.path para encontrar o client.py
current_dir = os.path.dirname(os.path.abspath(__file__)) # pasta web
main_project = os.path.dirname(current_dir) # pasta backend
backend_dir = os.path.join(main_project, 'backend')
sys.path.append(backend_dir)

import client

router = APIRouter() # Cria um roteador APIRouter para definir rotas separadas
app = FastAPI() # Cria uma instância da aplicação FastAPI

web_dir = os.path.dirname(os.path.abspath(__file__))  # Diretoria atual (web)   

app.mount("/web/html", StaticFiles(directory=os.path.join(web_dir, "html")), name="html")  # Monta a diretoria de ficheiros estáticos

templates = Jinja2Templates(directory=os.path.join(web_dir, "html"))
jump = 10  # Número de resultados por página
pageNum = 1  # Página atual



# Rota para a página inicial
@router.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """
    Rota para a página inicial.
    :param request: Objeto de solicitação.
    :return: Resposta HTML para a página inicial.

    Esta rota renderiza a página inicial da interface web.
    """
    return templates.TemplateResponse("index.html", {"request": request})

# Rota para indexar um URL manualmente
@router.post("/index_url")
async def index_url(url: str = Form(...)):
    """
    Rota para indexar um URL manualmente através da interface web.
    Lógica: conectar-se ao Gateway e chama a função submit_url do cliente para enviar o URL para indexação.

    :param url: URL a ser indexado.
    :return: Mensagem de sucesso.
    """


    config = load_config()
    gateway_ip = config['gateway']['ip']
    gateway_port = config['gateway']['port']
    
    client_instance = client.Client(gateway_ip, gateway_port)
    client_instance.submit_url(url)

    
    return {"message": f"URL {url} adicionado para indexação."}

#Rota para realizar buscas
#@router.get("/search_word", response_class = HTMLResponse)
@router.get("/search_word", response_class=JSONResponse)
async def search_word(request: Request, query: str = "", page: int = 1):
    """
    Rota para realizar buscas por palavras.

    Lógica: 
    1. Carrega a configuração do cliente para obter o IP e a porta do gateway.
    2. Conecta-se ao gateway.
    3. Chama a função search_word ou search_words do cliente, dependendo se é uma ou mais palavras.
    4. Implementa paginação  nos resultados, mostrando um número fixo de resultados por página (10 neste caso).
    5. Renderiza os resultados na página HTML.

    :param request: Objeto de solicitação.
    :param query: Palavra(s)-chave a ser(em) pesquisada(s).
    :param page: Número da página atual.
    :return: Resposta HTML com os resultados da pesquisa.

    Esta rota recebe uma consulta de pesquisa e retorna os resultados correspondentes.
    Os resultados são paginados, com um número fixo de resultados por página, 10 no caso.
    A função também carrega a configuração do cliente, conecta-se ao gateway e chama a função search_word ou search_words,
    dependendo se é uma ou se são mais palavras ao mesmo tempo a serem pesquisadas.
    Os resultados são então renderizados na página HTML.
    """
  
    global pageNum, jump
    print(f"Query recebida: {query}")

    search_results = []

    config = load_config()
    gate_ip = config['gateway']['ip']
    gate_port = config['gateway']['port']
    client_instance = client.Client(gate_ip, gate_port)
    

    results = client_instance.search(query)

    error_message = None

    if results is None:
        # Se falhou, define resultados como uma lista vazia e total como zero
        # Isto evita o TypeError e permite renderizar a página com uma mensagem de erro
        total_results = 0
        search_results = []
        # Opcional: Adicionar uma mensagem de erro ao contexto para ser exibida no template
        error_message = "Erro ao comunicar com o servidor de busca (Gateway/Barrels). Tente novamente mais tarde."
    else:
        # Se os resultados não forem None, calcula normalmente
        total_results = len(results)
        
        start = (page - 1) * jump
        end = start + jump
        search_results = results[start:end]

    
    #search_results = 
    print(f"Total de resultados: {total_results}")
    
    return templates.TemplateResponse(
        "search.html",
        {
            "request": request,
            "search_results": search_results,
            "query": query,
            "page": page,
            "total_results": total_results,
            "results_per_page": jump
        }
    )


# Rota para obter backlinks de um URL específico
@router.get("/get_backlinks", response_class=HTMLResponse)
async def get_backlinks(request: Request, url: str = "", page: int = 1, query: str = "", search_page: int = 1):
    """
    Rota para obter backlinks de um URL específico.

    Lógica:
    1. Carrega a configuração do cliente para obter o IP e a porta do gateway.
    2. Conecta-se ao gateway.
    3. Chama a função get_backlinks do cliente para obter os backlinks do URL fornecido.
    4. Implementa paginação nos resultados, mostrando um número fixo de resultados por página (10 neste caso).
    5. Renderiza os backlinks na página HTML.

    :param request: Objeto de solicitação.
    :param url: URL para o qual os backlinks são solicitados.
    :return: Resposta HTML com os backlinks.

    Esta rota recebe um URL e retorna os backlinks associados a ele.
    A função carrega a configuração do cliente, conecta-se ao gateway e chama a função get_backlinks.
    Os backlinks são então renderizados na página HTML.
    """
    global pageNum, jump

    backlinks = []

    config = load_config()
    gate_ip = config['gateway']['ip']
    gate_port = config['gateway']['port']
    client_instance = client.Client(gate_ip, gate_port)

    raw_backlinks = client_instance.search_backlinks(url)

    backlinks = [{"url": link} for link in raw_backlinks]

    print(f"Numero de backlinks recebidos para {url}: {len(backlinks)}")
    
    total_results = len(backlinks)
        
    start = (page - 1) * jump
    end = start + jump
    backlinks_result = backlinks[start:end]


    return templates.TemplateResponse(
        "backlinks.html",
        {
            "backlinks": backlinks_result,
            "page": page,
            "search_page": search_page,
            "total_results": total_results,
            "results_per_page": jump,
            "request": request,
            "url": url,
            "query": query
        }
    )


# Rota para integração com a API do Hacker News para histórias do usuário
@router.post("/hackernews/user-stories")
async def hacker_news_user_stories(querys_hn: dict = Body(...)):
    """
    Rota para ir buscar o top stories do Hacker News.

    Lógica:
    1. Carrega a configuração do cliente para obter o IP e a porta do gateway.
    2. Conecta-se ao gateway.
    3. Busca as top stories do Hacker News.
    4. Verifica se cada história contém as palavras-chave fornecidas na query.
    5. Se uma história contiver todas as palavras-chave, envia o URL para o gateway para indexação.

    :param query_dict: Dicionário que contém o que é para ser pesquisado.
    :return: Mensagem de sucesso ou erro.

    Depois de receber as top stories do Hacker News, vai buscar a query e vai verificar se existe alguma
    story que contenha as palavras desejadas para esse link ser então indexado.
    Faz uso da função submit_url do cliente para enviar o URL para o gateway.
    Se nada for indexado vai aparecer uma messagem que assim o diz.
    """

    config = load_config()
    gate_ip = config['gateway']['ip']
    gate_port = config['gateway']['port']
    client_instance = client.Client(gate_ip, gate_port)
    queries = querys_hn.get("query", "").split()
    num_indexed = 0
   
    try:
        top_stories = requests.get("https://hacker-news.firebaseio.com/v0/topstories.json").json()

        for ids in top_stories[:20]:
            story = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{ids}.json").json()
            text = story.get("text", " ")
            url = story.get("url", "")
            print(f"Story URL: {url}")

            num_queries_found = 0
            for query in queries:
                if query in text:
                    num_queries_found += 1


            if num_queries_found == len(queries):
                url = story.get("url", "")
                if url:
                    print(f"Indexing story with URL: {url}")
                    client_instance.submit_url(url)
                    num_indexed += 1

    except Exception as e:
        return {f"Erro ao buscar histórias do Hacker News: {str(e)}"}
    
    print(f"Total de histórias indexadas do Hacker News: {num_indexed}")

    return {
        "num_indexed": num_indexed,
    }

@router.post("/geminiAPI")
async def generate_analysis(data: dict = Body(...)):
  
    config = load_config()
    api_key = config['api_key']['key1']
    
    # Extrair dados do body
    search_queries = data.get("search_queries", [])
    snippets = data.get("snippets", [])
    
    # Construir o prompt para a API
    prompt = f"""Baseado nos seguintes termos de pesquisa e resultados, gere uma análise contextualizada e informativa.
    Termos de pesquisa: {', '.join(search_queries)} 
    Resultados obtidos:"""
    
    for i, snippet in enumerate(snippets, 1):
        title = snippet.get("title", "Sem título")
        citation = snippet.get("citation", "")
        url = snippet.get("url", "")
        prompt += f"\n{i}. {title}\n   URL: {url}\n   Citação: {citation}\n"
    
    prompt += """\n\nPor favor, forneça uma análise que:
    1. Resuma os principais tópicos encontrados
    2. Identifique padrões ou temas comuns
    3. Destaque informações relevantes e úteis
    4. Seja concisa e objetiva (máximo 300 palavras)"""
    
    
    client_genai = genai.Client(api_key=api_key)

    response = client_genai.models.generate_content(
        model="gemini-2.5-pro",
        messages=[
            {"role": "user", "content": prompt}
        ],
        max_output_tokens=500,
        temperature=0.7,
    )




    analysis = response.choices[0].message.content.strip()

    return {
        "analysis": analysis
    }

@router.websocket("/ws/stats")
async def websocket_endpoint(websocket: WebSocket):
    """
    Rota WebSocket para monitorização em tempo real das estatísticas do servidor.

    Lógica:
    1. Aceita a conexão WebSocket.
    2. Carrega a configuração do cliente para obter o IP e a porta do gateway.
    3. Conecta-se ao gateway.
    4. Num loop infinito, vê as estatísticas do servidor a cada 3 segundos e envia os dados ao cliente via WebSocket.

    :param websocket: Objeto WebSocket para comunicação em tempo real.
    """
    await websocket.accept() # aceitar a conexão

    # configuração do cliente
    config = load_config()
    gateway_ip = config['gateway']['ip']
    gateway_port = config['gateway']['port']
    client_instance = client.Client(gateway_ip, gateway_port)

    try:
        while True:
            stats_data = client_instance.statistics()
            await websocket.send_json(stats_data)
            await asyncio.sleep(5) # espera 3 seg para não sobrecarregar

    except WebSocketDisconnect:
        print("WebSocket desconectado")
    except Exception as e:
        print(f"Erro no WebSocket: {str(e)}")
    

def load_config():
    """
    - Função para carregar as configurações dos ips e portas para o gateway a partir de um ficheiro JSON.
    """
    with open('../backend/config.json', 'r') as f:
        config = json.load(f)
    return config



app.include_router(router) # Inclui as rotas na aplicação FastAPI