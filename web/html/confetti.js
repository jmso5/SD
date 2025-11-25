/**
 * Cria efeito de confetti na posição especificada
 * @param {number} x - Posição X do confetti
 * @param {number} y - Posição Y do confetti
 */
function createConfetti(x, y) {
    for (let i = 0; i < 20; i++) {
        const confetti = document.createElement('div');
        confetti.style.position = 'fixed';
        confetti.style.left = x + 'px';
        confetti.style.top = y + 'px';
        confetti.style.width = '10px';
        confetti.style.height = '10px';
        confetti.style.backgroundColor = `hsl(${Math.random() * 360}, 100%, 50%)`;
        confetti.style.borderRadius = '50%';
        confetti.style.pointerEvents = 'none';
        confetti.style.zIndex = '9999';
        document.body.appendChild(confetti);
        
        const angle = Math.random() * Math.PI * 2;
        const velocity = 5 + Math.random() * 5;
        const vx = Math.cos(angle) * velocity;
        const vy = Math.sin(angle) * velocity - 10;
        
        let currentX = x;
        let currentY = y;
        let currentVY = vy;
        
        const animation = setInterval(() => {
            currentX += vx;
            currentY += currentVY;
            currentVY += 0.5;
            
            confetti.style.left = currentX + 'px';
            confetti.style.top = currentY + 'px';
            
            if (currentY > window.innerHeight) {
                clearInterval(animation);
                confetti.remove();
            }
        }, 20);
    }
}

/**
 * Inicializa o efeito de confetti no hover da imagem do logo
 */
function initConfettiEffect() {
    const logoImg = document.getElementById('logo-img');
    
    if (!logoImg) {
        console.error('Logo image not found. Make sure the image has id="logo-img"');
        return;
    }
    
    let confettiInterval = null;
    
    logoImg.addEventListener('mouseenter', () => {
        // Iniciar confetti contínuo
        confettiInterval = setInterval(() => {
            const rect = logoImg.getBoundingClientRect();
            const centerX = rect.left + rect.width / 2;
            const centerY = rect.top + rect.height / 2;
            createConfetti(centerX, centerY);
        }, 300); // Cria confetti a cada 300ms (mais lento)
    });
    
    logoImg.addEventListener('mouseleave', () => {
        // Parar confetti quando o mouse sai
        if (confettiInterval) {
            clearInterval(confettiInterval);
            confettiInterval = null;
        }
    });
}

// Inicializar quando o DOM estiver pronto
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initConfettiEffect);
} else {
    initConfettiEffect();
}