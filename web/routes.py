"""
Módulo responsável por definir as rotas da interface web do cliente.

Este ficheiro usa FastAPI em conjunto com templates Jinja2 para gerir:
- A página inicial e interface de pesquisa
- Submissão e indexação manual de URLs
- Sistema de paginação de resultados
- Obtenção de backlinks
- Integração com a API pública do Hacker News
- Geração de análises através da API Gemini
- Endpoint WebSocket para estatísticas em tempo real do gateway
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
from fastapi import Body, WebSocket, WebSocketDisconnect, BackgroundTasks
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
    Rota que renderiza a página inicial da interface web.

    Args: 
        request (Request): Objeto de requisição FastAPI.
    Returns: 
        TemplateResponse: Resposta HTML renderizada com o template "index.html".
    """
    return templates.TemplateResponse("index.html", {"request": request})

# Rota para indexar um URL manualmente
@router.post("/index_url")
async def index_url(url: str = Form(...)):
    """
    Rota para indexar manualmente um URL no sistema através da gateway.
    É criada uma instância do cliente que se conecta ao gateway e envia o URL para indexação.

    Args:
        url (str): URL a ser indexado.
    Returns:
        dict: Mensagem de sucesso ou erro.
    """


    config = load_config()
    gateway_ip = config['gateway']['ip']
    gateway_port = config['gateway']['port']
    
    try:
        client_instance = client.Client(gateway_ip, gateway_port)
        client_instance.submit_url(url)
    except Exception as e:
        return {"message": "Não foi possível realizar a indexação. Por favor, tente novamente mais tarde."}

    
    return {"message": f"URL {url} adicionado para indexação."}

#Rota para realizar buscas
#@router.get("/search_word", response_class = HTMLResponse)
@router.get("/search_word", response_class=JSONResponse)
async def search_word(request: Request, query: str = "", page: int = 1):
    """
    Rota para executar uma pesquisa no sistema de indexação e devolve os resultados paginados.
    É criada uma instância do cliente que se conecta ao gateway e realiza a pesquisa.
    É verificada a existência de resultados antes de calcular a paginação para evitar erros.

    Args:
        request (Request): Objeto de requisição FastAPI.
        query (str): Termo de pesquisa.
        page (int): Número da página de resultados a ser exibida.
    Returns:
        TemplateResponse: Resposta HTML renderizada com o template "search.html" contendo os resultados da pesquisa.
    """
  
    global pageNum, jump
    print(f"Query recebida: {query}")

    search_results = []

    config = load_config()
    gate_ip = config['gateway']['ip']
    gate_port = config['gateway']['port']
    client_instance = client.Client(gate_ip, gate_port)
    
    try:
        results = client_instance.search(query)
    except Exception as e:
        results = None
        print("Não foi possível realizar a pesquisa neste momento.")

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
    Rota que obtem os backlinks de um URL específico e apresenta-os com paginação.

    Args:
        request (Request): Objeto de requisição FastAPI.
        url (str): URL para o qual os backlinks serão obtidos.
        page (int): Número da página de resultados de backlinks a ser exibida.
        query (str): Termo de pesquisa original.
        search_page (int): Página de pesquisa prévia.
    
    Returns:
        TemplateResponse: Resposta HTML renderizada com o template "backlinks.html" contendo os backlinks.
    """

    global pageNum, jump

    backlinks = []

    config = load_config()
    gate_ip = config['gateway']['ip']
    gate_port = config['gateway']['port']
    client_instance = client.Client(gate_ip, gate_port)

    try:
        raw_backlinks = client_instance.search_backlinks(url)
    except Exception as e:
        raw_backlinks = []
        print("Não foi possível obter os backlinks neste momento.")

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
    Integra com a API do Hacker News para procurar histórias cujo texto contenha todos os termos fornecidos e enviá-las para indexação.
    É utilizada a API pública do Hacker News para obter as histórias mais recentes.
    Cada história é verificada para ver se o texto contém todos os termos de pesquisa fornecidos.
    Se corresponderem, o URL da história é enviado para indexação através de uma instancia do cliente.
    
    Args:
        querys_hn (dict): Dicionário contendo os termos de pesquisa sob a chave "query".
    Returns:
        dict: Número de histórias indexadas.
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
    """
    Gera uma análise contextualizada baseada em termos de pesquisa e snippets fornecidos, utilizando a API Gemini.
    Utiliza a biblioteca genai para interagir com a API Gemini.
    Constrói um prompt detalhado com os termos de pesquisa e snippets, solicitando uma análise concisa e informativa.

    Args:
        data (dict): Dicionário contendo "search_queries" e "snippets".
    Returns:
        dict: Análise gerada pela API Gemini.
    """
  
    config = load_config()
    api_key = config['api_key']['key1']
    
    # Extrair dados do body
    search_queries = data.get("search_queries", [])
    snippets = data.get("snippets", [])
    
    # Construir o prompt para a API
    prompt = (f"Baseado nos seguintes termos de pesquisa e resultados, gere uma análise contextualizada e informativa.\n"
        f"Termos de pesquisa: {', '.join(search_queries)}\n" 
        "Resultados obtidos: \n")
    
    for i, snippet in enumerate(snippets, 1):
        title = snippet.get("title", "Sem título")
        citation = snippet.get("citation", "")
        url = snippet.get("url", "")
        prompt += f"\n{i}. {title}\n   URL: {url}\n   Citação: {citation}\n"
    
    prompt += ("Por favor, forneça uma análise que:\n"
        "1. Resuma os principais tópicos\n"
        "2. Identifique padrões comuns\n"
        "3. Destaque informações úteis\n"
        "4. Seja concisa e objetiva (máx 300 palavras)")
    
    # cliente gemini
    client_genai = genai.Client(api_key=api_key)
    
    response = client_genai.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt
    )

    return {
        "analysis": response.text
    }



@router.websocket("/ws/stats")
async def websocket_endpoint(websocket: WebSocket):
    """
    Mantém uma conexão WebSocket para enviar estatísticas em tempo real do gateway sempre que houver alterações.

    Args:
        websocket (WebSocket): Conexão WebSocket com o cliente.
    Envia:
        JSON: Estatísticas atualizadas do gateway.
    """
    await websocket.accept() # aceitar a conexão

    # configuração do cliente
    config = load_config()
    gateway_ip = config['gateway']['ip']
    gateway_port = config['gateway']['port']
    client_instance = client.Client(gateway_ip, gateway_port)

    last_stats_data = {}

    try:
        while True:
            # obter estatisticas atuais do gateway
            current_stats_data = client_instance.statistics()

            # verifica se houve mudanças nas estatísticas
            if current_stats_data != last_stats_data:
                await websocket.send_json(current_stats_data) # enviar dados atualizados 
                print("WebSocket: Estatísticas enviadas.")

                last_stats_data = current_stats_data # atualizar os dados das estatísticas

            await asyncio.sleep(5)  # esperar antes de verificar novamente

    except WebSocketDisconnect:
        print("WebSocket desconectado")
    except Exception as e:
        print(f"Erro no WebSocket: {str(e)}")

def load_config():
    """
    - Função para carregar as configurações dos ips e portas para o gateway a partir de um ficheiro JSON.
    """
    with open('../backend/config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    return config


@router.get("/offline", response_class=HTMLResponse)
async def offline_page():
    """
    Renderiza a página fallback quando o servidor está offline.

    Returns:
        HTMLResponse: Conteúdo HTML da página offline.
    """
    
    web_dir = "path/to/web/directory"  # Define the path to the web directory
    try:
        with open(os.path.join(web_dir, "html", "offline.html"), "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except Exception as e:
        return HTMLResponse(content="<h1>Offline - Servidor Indisponível</h1><p>Por favor, tente novamente mais tarde.</p>")

@router.get("/api/health")
async def health_check():
    """
    Endpoint para verificar se o servidor está online.
    Utilizado pelo service worker para detectar o status da conexão.
    """
    return JSONResponse(content={"status": "online"})



app.include_router(router) # Inclui as rotas na aplicação FastAPI