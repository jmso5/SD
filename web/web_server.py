import grpc
import index_pb2
import index_pb2_grpc
from google.protobuf import empty_pb2

import json
import sys
import os
import asyncio
import httpx # Para chamadas a APIs REST (Hacker News, OpenAI)

from fastapi import FastAPI, Request, Form, WebSocket, WebSocketDisconnect
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.status import HTTP_303_SEE_OTHER



# --- 1. Configuração Inicial e Conexão gRPC ---

# Carrega a configuração
def load_config(file_path='../backend/config.json'):
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERRO: Ficheiro de configuração '{file_path}' não encontrado.")
        sys.exit(1)

# Configuração da Aplicação FastAPI
app = FastAPI()

# Montar diretórios estáticos (para CSS, JS) e templates (para HTML)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="html")

# Carregar config e estabelecer conexão gRPC com o Gateway
config = load_config()
gateway_addr = f"{config['gateway']['ip']}:{config['gateway']['port']}"

# Opções para mensagens gRPC grandes (igual ao client.py)
MAX_MESSAGE_LENGTH = 262144000
options = [
    ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
    ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
]
channel = grpc.insecure_channel(gateway_addr, options=options)
gateway_stub = index_pb2_grpc.GatewayStub(channel) # Stub global

# Cliente HTTP assíncrono para APIs REST
http_client = httpx.AsyncClient(timeout=10.0)

print(f"Servidor Web iniciado. A ligar ao Gateway em {gateway_addr}...")

# --- 2. Rotas Principais da Interface Web (HTML) ---

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """
    Renderiza a página inicial (index.html) que contém
    o formulário de pesquisa e o de submissão de URL.
    """
    # O template "index.html" vai carregar as estatísticas via WebSocket
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/submit-url", response_class=RedirectResponse)
async def handle_submit_url(request: Request, url: str = Form(...)):
    """
    Recebe um URL do formulário da página inicial (Requisito 1).
    Chama o putNew no Gateway e redireciona de volta para a home.
    """
    if url.startswith("http://") or url.startswith("https://"):
        try:
            # Chama o gRPC do Gateway, tal como o client.py fazia
            gateway_stub.putNew(index_pb2.PutNewRequest(url=url))
            print(f"[WEB] URL submetido para indexação: {url}")
        except grpc.RpcError as e:
            print(f"Erro gRPC ao submeter URL: {e}")
            
    # Redireciona para a página inicial (padrão PRG - Post-Redirect-Get)
    return RedirectResponse(url="/", status_code=HTTP_303_SEE_OTHER)

@app.get("/search", response_class=HTMLResponse)
async def handle_search(request: Request, q: str, page: int = 1):
    """
    Recebe uma consulta de pesquisa (Requisito 3).
    Chama o gRPC de pesquisa no Gateway e renderiza a página de resultados.
    """
    words = q.lower().split()
    results = []
    
    if not words:
        # Se a pesquisa for vazia, não faz nada
        return templates.TemplateResponse("search_results.html", {
            "request": request, "query": q, "results": [], "page": 1, 
            "total_pages": 1, "openai_analysis": "", "hn_url": ""
        })

    try:
        # Lógica de pesquisa (igual ao client.py)
        if len(words) == 1:
            response = gateway_stub.searchWord(index_pb2.SearchWordRequest(word=words[0]))
        else:
            response = gateway_stub.searchVariousWords(index_pb2.SearchVariousWordsRequest(words=words))
        
        results = response.results
        print(f"[WEB] Pesquisa por '{q}' encontrou {len(results)} resultados.")

    except grpc.RpcError as e:
        print(f"Erro gRPC ao pesquisar: {e}")
        
    # Lógica de Paginação (Requisito 3 - 10 resultados por página) [cite: 74]
    results_per_page = 10
    start_index = (page - 1) * results_per_page
    end_index = start_index + results_per_page
    paginated_results = results[start_index:end_index]
    total_pages = (len(results) + results_per_page - 1) // results_per_page

    # --- Integração REST (Requisito 6) ---
    
    # 1. OpenAI: Gerar análise 
    openai_analysis = await get_openai_analysis(q)
    
    # 2. Hacker News: URL para ação de indexar
    hn_action_url = f"/index-hackernews?q={q}"

    return templates.TemplateResponse("search_results.html", {
        "request": request,
        "query": q,
        "results": paginated_results,
        "page": page,
        "total_pages": total_pages,
        "openai_analysis": openai_analysis,
        "hn_url": hn_action_url
    })

@app.get("/backlinks", response_class=HTMLResponse)
async def handle_get_backlinks(request: Request, url: str):
    """
    Mostra os backlinks para um dado URL (Requisito 5).
    """
    backlinks = []
    try:
        response = gateway_stub.getBacklinks(index_pb2.PutNewRequest(url=url))
        backlinks = response.backlinks
        print(f"[WEB] Pedido de backlinks para '{url}' encontrou {len(backlinks)} links.")
    except grpc.RpcError as e:
        print(f"Erro gRPC ao obter backlinks: {e}")

    return templates.TemplateResponse("backlinks.html", {
        "request": request,
        "url": url,
        "backlinks": backlinks
    })

# --- 3. WebSocket para Estatísticas em Tempo Real ---

@app.websocket("/ws/stats")
async def websocket_stats(websocket: WebSocket):
    """
    Envia estatísticas atualizadas para o cliente via WebSocket (Requisito 6)[cite: 86, 92, 94].
    """
    await websocket.accept()
    print("[WEB] Cliente WebSocket de estatísticas conectado.")
    try:
        while True:
            try:
                # Chama o gRPC getStatistics do Gateway [cite: 96]
                stats = gateway_stub.getStatistics(empty_pb2.Empty())
                
                # Formata os dados para JSON
                data = {
                    "top_searches": list(stats.top_searches),
                    "barrel_stats": [
                        {
                            "id": s.barrel_id,
                            "size": s.index_size,
                            "avg_time": round(s.avg_response_time, 2)
                        } for s in stats.barrel_stats
                    ]
                }
                
                # Envia os dados para o cliente (browser)
                await websocket.send_json(data)
                
            except grpc.RpcError as e:
                print(f"Erro gRPC no loop do WebSocket: {e}")
                # Envia erro para o cliente, se desejar
                await websocket.send_json({"error": "Gateway indisponível"})

            # Atualiza a cada 5 segundos (para não sobrecarregar)
            await asyncio.sleep(5)
            
    except WebSocketDisconnect:
        print("[WEB] Cliente WebSocket de estatísticas desconectado.")
    except Exception as e:
        print(f"Erro inesperado no WebSocket: {e}")

# --- 4. Rotas de Integração REST (Hacker News & OpenAI) ---

async def get_openai_analysis(query: str) -> str:
    """
    Contacta a API da OpenAI para gerar uma análise contextual (Requisito 6).
    """
    api_key = os.environ.get("OPENAI_API_KEY") # NECESSÁRIO!
    if not api_key:
        return "Análise da OpenAI indisponível (OPENAI_API_KEY não configurada no servidor)."

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    json_data = {
        "model": "gpt-3.5-turbo",
        "messages": [
            {
                "role": "system",
                "content": "És um assistente de motor de busca. Faz uma análise textual curta (1-2 frases) sobre o impacto ou relevância dos termos de pesquisa."
            },
            {
                "role": "user",
                "content": query
            }
        ],
        "temperature": 0.5,
        "max_tokens": 60
    }

    try:
        response = await http_client.post("https://api.openai.com/v1/chat/completions", headers=headers, json=json_data)
        response.raise_for_status() # Lança exceção se for 4xx ou 5xx
        data = response.json()
        return data['choices'][0]['message']['content'].strip()
    except Exception as e:
        print(f"[OpenAI] Erro ao contactar API: {e}")
        return f"Análise da OpenAI falhou: {e}"

@app.get("/index-hackernews", response_class=RedirectResponse)
async def index_hackernews(request: Request, q: str):
    """
    Indexa URLs das 'top stories' do Hacker News que contenham os termos
    da pesquisa (Requisito 6)[cite: 100, 101].
    """
    print(f"[WEB] A indexar Top Stories do Hacker News para: '{q}'")
    search_terms = q.lower().split()
    
    try:
        # 1. Obter IDs das Top Stories
        hn_api_url = "https://hacker-news.firebaseio.com/v0"
        top_stories_ids_response = await http_client.get(f"{hn_api_url}/topstories.json")
        top_stories_ids = top_stories_ids_response.json()

        urls_found = 0
        
        # 2. Verificar as primeiras 30 histórias
        for story_id in top_stories_ids[:30]:
            story_response = await http_client.get(f"{hn_api_url}/item/{story_id}.json")
            story = story_response.json()
            
            story_title = story.get('title', '').lower()
            
            # 3. Verificar se o título contém TODOS os termos da pesquisa
            if all(term in story_title for term in search_terms):
                if 'url' in story:
                    story_url = story['url']
                    # 4. Enviar URL para o Gateway indexar
                    gateway_stub.putNew(index_pb2.PutNewRequest(url=story_url))
                    print(f"[HackerNews] Encontrado e submetido: {story_url}")
                    urls_found += 1
        
        print(f"[HackerNews] Submetidos {urls_found} novos URLs.")
        
    except Exception as e:
        print(f"[HackerNews] Erro ao contactar API: {e}")

    # Redireciona de volta para a pesquisa original
    return RedirectResponse(url=f"/search?q={q}", status_code=HTTP_303_SEE_OTHER)


# --- 5. Ponto de Entrada ---

if __name__ == "__main__":
    import uvicorn
    # Corre o servidor web na porta 8000
    # 'reload=True' é ótimo para desenvolvimento
    uvicorn.run("web_server:app", host="0.0.0.0", port=8000, reload=True)