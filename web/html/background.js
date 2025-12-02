    class RealisticLightning {
        constructor(container) {
            this.container = container;
            this.screenFlash = document.getElementById('screen-flash');
            this.mindFlayer = document.getElementById('mind-flayer');
            this.mindFlayerGlow = document.getElementById('mind-flayer-glow');
        }

        // Gerar ponto com variação aleatória
        randomOffset(base, variation) {
            return base + (Math.random() - 0.5) * variation;
        }

        // Criar caminho de raio com ramificações
        generateLightningPath(startX, startY, endY, segments = 20) {
            let path = `M ${startX} ${startY} `;
            let currentX = startX;
            let currentY = startY;
            
            const segmentHeight = (endY - startY) / segments;
            const branches = [];

            for (let i = 1; i <= segments; i++) {
                const nextY = startY + (segmentHeight * i);
                const nextX = this.randomOffset(currentX, 30);
                
                path += `L ${nextX} ${nextY} `;
                
                if (Math.random() > 0.7 && i < segments - 3) {
                    branches.push({
                        startX: nextX,
                        startY: nextY,
                        direction: Math.random() > 0.5 ? 1 : -1
                    });
                }
                
                currentX = nextX;
                currentY = nextY;
            }

            return { mainPath: path, branches };
        }

        // Criar ramificação
        generateBranch(startX, startY, direction, length = 5) {
            let path = `M ${startX} ${startY} `;
            let currentX = startX;
            let currentY = startY;
            
            const segmentLength = 15;

            for (let i = 1; i <= length; i++) {
                const nextY = currentY + segmentLength;
                const nextX = currentX + (direction * this.randomOffset(20, 15));
                
                path += `L ${nextX} ${nextY} `;
                
                currentX = nextX;
                currentY = nextY;
            }

            return path;
        }

        // Criar elemento SVG
        createSVG() {
            const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
            svg.classList.add('lightning-svg');
            svg.setAttribute('viewBox', '0 0 1920 1080');
            svg.style.position = 'absolute';
            svg.style.width = '100%';
            svg.style.height = '100%';

            const filterId = `glow-${Date.now()}-${Math.random()}`;
            const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
            defs.innerHTML = `
                <filter id="${filterId}" x="-50%" y="-50%" width="200%" height="200%">
                    <feGaussianBlur in="SourceGraphic" stdDeviation="4" result="blur"/>
                    <feColorMatrix in="blur" type="matrix" values="
                        1 0 0 0 0
                        0 0.2 0 0 0
                        0 0 0.2 0 0
                        0 0 0 2 0" result="glow"/>
                    <feMerge>
                        <feMergeNode in="glow"/>
                        <feMergeNode in="glow"/>
                        <feMergeNode in="SourceGraphic"/>
                    </feMerge>
                </filter>
            `;
            svg.appendChild(defs);
            svg.filterId = filterId;

            return svg;
        }

        // Revelar Mind Flayer
        revealMindFlayer(intensity = 1) {
            // Intensidade: 1 = normal, 2 = intensa
            if (intensity > 1.5) {
                this.mindFlayer.classList.add('visible', 'intense');
                this.mindFlayerGlow.classList.add('visible');
            } else {
                this.mindFlayer.classList.add('visible');
                this.mindFlayer.classList.remove('intense');
            }
        }

        // Esconder Mind Flayer
        hideMindFlayer() {
            this.mindFlayer.classList.remove('visible', 'intense');
            this.mindFlayerGlow.classList.remove('visible');
        }

        // Animar raio
        animateLightning(path, duration = 200, intensity = 1) {
            const animate = () => {
                // Primeiro flash - REVELAR MIND FLAYER
                path.style.opacity = '1';
                path.style.strokeWidth = '4';
                this.flashScreen(0.25 * intensity);
                this.revealMindFlayer(intensity);

                setTimeout(() => {
                    path.style.opacity = '0';
                    this.hideMindFlayer();
                }, 50);

                // Segundo flash
                setTimeout(() => {
                    path.style.opacity = '0.7';
                    path.style.strokeWidth = '3';
                    this.flashScreen(0.15 * intensity);
                    this.revealMindFlayer(intensity * 0.8);
                }, 80);

                setTimeout(() => {
                    path.style.opacity = '0';
                    this.hideMindFlayer();
                }, 130);

                // Terceiro flash
                setTimeout(() => {
                    path.style.opacity = '0.4';
                    path.style.strokeWidth = '2';
                    this.flashScreen(0.08 * intensity);
                    this.revealMindFlayer(intensity * 0.5);
                }, 160);

                setTimeout(() => {
                    path.style.opacity = '0';
                    this.hideMindFlayer();
                }, duration);
            };

            animate();
        }

        // Flash de tela
        flashScreen(intensity) {
            this.screenFlash.style.opacity = intensity;
            setTimeout(() => {
                this.screenFlash.style.opacity = '0';
            }, 50);
        }

        // Criar raio completo
        strike(intense = false) {
            const svg = this.createSVG();
            
            const startX = Math.random() * 1920;
            const startY = -50;
            const endY = 1080 + 50;

            const { mainPath, branches } = this.generateLightningPath(startX, startY, endY, 25);

            const mainPathElement = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            mainPathElement.setAttribute('d', mainPath);
            mainPathElement.classList.add('lightning-path');
            
            const intensity = intense ? 2 : 1;
            const colorIntensity = intense ? 255 : Math.random() * 100 + 150;
            
            mainPathElement.style.stroke = `rgba(${colorIntensity}, ${colorIntensity * 0.6}, ${colorIntensity * 0.6}, 1)`;
            mainPathElement.style.filter = `url(#${svg.filterId})`;
            svg.appendChild(mainPathElement);

            branches.forEach((branch, index) => {
                const branchPath = this.generateBranch(
                    branch.startX, 
                    branch.startY, 
                    branch.direction, 
                    Math.floor(Math.random() * 4) + 3
                );
                
                const branchElement = document.createElementNS('http://www.w3.org/2000/svg', 'path');
                branchElement.setAttribute('d', branchPath);
                branchElement.classList.add('lightning-path');
                branchElement.style.stroke = `rgba(255, ${Math.random() * 80 + 170}, ${Math.random() * 80 + 170}, 0.9)`;
                branchElement.style.strokeWidth = intense ? '3' : '2';
                branchElement.style.filter = `url(#${svg.filterId})`;
                svg.appendChild(branchElement);

                setTimeout(() => {
                    this.animateLightning(branchElement, 180, intensity * 0.7);
                }, index * 15);
            });

            this.container.appendChild(svg);
            this.animateLightning(mainPathElement, 200, intensity);

            setTimeout(() => {
                svg.remove();
            }, 500);
        }

        // Iniciar tempestade
        startStorm() {
            const strikeInterval = setInterval(() => {
                if (Math.random() > 0.6) {
                    // 15% chance de raio super intenso
                    const isIntense = Math.random() > 0.85;
                    this.strike(isIntense);
                    
                    // Sequência de raios
                    if (Math.random() > 0.7) {
                        setTimeout(() => this.strike(false), 150);
                        
                        // Triplo raio ocasional
                        if (Math.random() > 0.9) {
                            setTimeout(() => this.strike(true), 300);
                        }
                    }
                }
            }, 3000);

            // Raios iniciais dramáticos
            setTimeout(() => this.strike(false), 500);
            setTimeout(() => this.strike(true), 1200);
            setTimeout(() => this.strike(false), 1400);
            setTimeout(() => this.strike(false), 2500);

            return strikeInterval;
        }
    }

    // Inicializar tempestade
    document.addEventListener('DOMContentLoaded', () => {
        const container = document.getElementById('lightning-container');
        const lightning = new RealisticLightning(container);
        lightning.startStorm();
    });

