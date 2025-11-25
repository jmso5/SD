"""
Este ficheiro guarda as rotas para a interface web do cliente.
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
    - Rota para indexar um URL manualmente através da interface web.
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
    Rota para realizar buscas.
    :param request: Objeto de solicitação.
    :param query: Palavra(s)-chave a ser(em) pesquisada(s).
    :param page: Número da página atual.
    :return: Resposta HTML com os resultados da pesquisa.

    Esta rota recebe uma consulta de pesquisa e retorna os resultados correspondentes.
    Os resultados são paginados, com um número fixo de resultados por página, 10 no caso.
    A função também carrega a configuração do cliente, conecta-se ao gateway e chama a função search_word ou search_words
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

    """
    return {
        "status": "OK" if total_results > 0 or not error_message else "ERROR",
        "query": query,
        "total_results": total_results,
        "results_per_page": jump,
        "error_message": error_message,
        "search_results": [
            {
                "url": r.url,
                "title": r.title,
                "text": r.text,
                "links_count": r.links_count
            } 
            for r in search_results
        ]
    }"""


# Rota para obter backlinks de um URL específico
@router.get("/get_backlinks", response_class=HTMLResponse)
async def get_backlinks(request: Request, url: str = "", page: int = 1, query: str = "", search_page: int = 1):
    """
    Rota para obter backlinks de um URL específico.
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

def load_config():
    """
    - Função para carregar as configurações dos ips e portas para o gateway a partir de um ficheiro JSON.
    """
    with open('../backend/config.json', 'r') as f:
        config = json.load(f)
    return config



app.include_router(router) # Inclui as rotas na aplicação FastAPI