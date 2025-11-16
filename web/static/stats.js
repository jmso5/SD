// Espera que o DOM esteja pronto
document.addEventListener("DOMContentLoaded", function() {
    
    const statsContainer = document.getElementById("stats-container");
    
    // Tenta ligar ao WebSocket
    // Garante que o 'host' está correto (window.location.host)
    const ws = new WebSocket(`ws://${window.location.host}/ws/stats`);

    ws.onopen = () => {
        console.log("WebSocket de estatísticas conectado.");
    };

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        
        if (data.error) {
            statsContainer.innerHTML = `<p>Erro: ${data.error}</p>`;
            return;
        }

        // 1. Top 10 Pesquisas
        let topSearchesHtml = "<h3>Top 10 Pesquisas</h3>";
        if (data.top_searches && data.top_searches.length > 0) {
            topSearchesHtml += "<ol>";
            data.top_searches.forEach(search => {
                topSearchesHtml += `<li>${search}</li>`;
            });
            topSearchesHtml += "</ol>";
        } else {
            topSearchesHtml += "<p>Nenhuma pesquisa registada.</p>";
        }

        // 2. Estatísticas dos Barrels
        let barrelStatsHtml = "<h3>Estado dos Barrels</h3>";
        if (data.barrel_stats && data.barrel_stats.length > 0) {
            barrelStatsHtml += "<table><thead><tr><th>Barrel ID</th><th>Tamanho do Índice</th><th>Tempo Médio (ds)</th></tr></thead><tbody>";
            data.barrel_stats.forEach(barrel => {
                barrelStatsHtml += `<tr>
                    <td>${barrel.id}</td>
                    <td>${barrel.size}</td>
                    <td>${barrel.avg_time}</td>
                </tr>`;
            });
            barrelStatsHtml += "</tbody></table>";
        } else {
            barrelStatsHtml += "<p>Nenhum barrel ativo.</p>";
        }

        // Atualiza o HTML
        statsContainer.innerHTML = topSearchesHtml + barrelStatsHtml;
    };

    ws.onclose = () => {
        console.log("WebSocket de estatísticas desconectado. A tentar reconectar em 5s...");
        statsContainer.innerHTML = "<p>A tentar reconectar ao servidor de estatísticas...</p>";
        // Tenta reconectar após 5 segundos
        setTimeout(() => {
            // Este é um reinício simples, recarregando o script ou a página.
            // Numa app real, usaria lógica de reconexão mais robusta.
            window.location.reload(); 
        }, 5000);
    };

    ws.onerror = (error) => {
        console.error("Erro no WebSocket:", error);
        statsContainer.innerHTML = "<p>Erro na ligação ao servidor de estatísticas.</p>";
    };
});