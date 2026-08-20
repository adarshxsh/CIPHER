document.addEventListener('DOMContentLoaded', () => {
    const slides = document.querySelectorAll('.slide');
    const notesView = document.getElementById('notes-view');
    const notesContainer = document.getElementById('notes-container');
    const notesNum = document.getElementById('notes-num');
    
    const btnPrev = document.getElementById('btn-prev');
    const btnNext = document.getElementById('btn-next');
    const btnNotes = document.getElementById('btn-notes');
    
    const presentationFrame = document.getElementById('presentation-frame');
    
    let currentSlide = 0;
    
    // Terminal mock logs for slide 15
    const terminalContent = document.getElementById('term-logs');
    const terminalLogs = [
        { text: "[INFO] content: Ingested file, created 16 chunks", type: "info" },
        { text: "[INFO] manifest: Merkle root computed = a7b8...4f1e", type: "success" },
        { text: "[INFO] control: Registering 24 shards on PlacementMap", type: "info" },
        { text: "[INFO] transport: fall back relay host active: PeerID=12D3K...", type: "info" },
        { text: "[INFO] dcutr: upgrading relay stream to target peer...", type: "info" },
        { text: "[INFO] dcutr: UDP hole punch succeeded! Direct connection established", type: "success" },
        { text: "[INFO] scheduler: starting parallel swarm retrials", type: "info" },
        { text: "[INFO] worker-0: downloading shard 0 from Provider A... OK", type: "success" },
        { text: "[INFO] worker-1: downloading shard 1 from Provider B... OK", type: "success" },
        { text: "[INFO] verifier: SHA-256 match for shard 0 & 1", type: "success" },
        { text: "[INFO] engine: verification succeeded, file reconstructed", type: "success" }
    ];
    let terminalInterval = null;

    // Scale function to keep the slide frame in 16:9 aspect ratio centered
    function scaleFrame() {
        if (!presentationFrame) return;
        const parent = presentationFrame.parentElement;
        const parentWidth = parent.clientWidth;
        const parentHeight = parent.clientHeight;
        
        // Base dimensions: 1200x675
        const scaleX = parentWidth / 1200;
        const scaleY = parentHeight / 675;
        const scale = Math.min(scaleX, scaleY) * 0.95; // 5% padding margin
        
        presentationFrame.style.transform = `scale(${scale})`;
    }

    function showSlide(index) {
        if (index < 0) index = 0;
        if (index >= slides.length) index = slides.length - 1;
        
        slides[currentSlide].classList.remove('active');
        currentSlide = index;
        slides[currentSlide].classList.add('active');
        
        // Update Speaker Notes
        const activeSlideNotes = slides[currentSlide].querySelector('.slide-notes');
        if (activeSlideNotes) {
            notesContainer.innerHTML = activeSlideNotes.innerHTML;
        } else {
            notesContainer.innerHTML = "<p>No notes for this slide.</p>";
        }
        notesNum.innerText = (currentSlide + 1);
        
        // Terminal log trigger
        if (currentSlide === 14) { // Index 14 is Slide 15
            startTerminalDemo();
        } else {
            stopTerminalDemo();
        }

        // Re-scale on slide change
        scaleFrame();
    }
    
    function navigateNext() {
        if (currentSlide < slides.length - 1) {
            showSlide(currentSlide + 1);
        }
    }
    
    function navigatePrev() {
        if (currentSlide > 0) {
            showSlide(currentSlide - 1);
        }
    }
    
    function toggleNotes() {
        notesView.classList.toggle('collapsed');
        // Trigger scaling after layout finishes resizing notes panel
        setTimeout(scaleFrame, 150);
    }
    
    function startTerminalDemo() {
        if (!terminalContent) return;
        terminalContent.innerHTML = "";
        stopTerminalDemo();
        
        let logIndex = 0;
        terminalInterval = setInterval(() => {
            if (logIndex < terminalLogs.length) {
                const log = terminalLogs[logIndex];
                const line = document.createElement('div');
                line.className = `term-line ${log.type}`;
                line.innerText = log.text;
                terminalContent.appendChild(line);
                
                // Auto scroll terminal
                const termWrapper = terminalContent.closest('.mock-terminal');
                if (termWrapper) {
                    termWrapper.scrollTop = termWrapper.scrollHeight;
                }
                logIndex++;
            } else {
                clearInterval(terminalInterval);
            }
        }, 800);
    }
    
    function stopTerminalDemo() {
        if (terminalInterval) {
            clearInterval(terminalInterval);
            terminalInterval = null;
        }
        if (terminalContent) {
            terminalContent.innerHTML = "";
        }
    }
    
    // Key listeners
    document.addEventListener('keydown', (e) => {
        switch (e.key) {
            case 'ArrowRight':
            case ' ':
            case 'Enter':
            case 'PageDown':
                e.preventDefault();
                navigateNext();
                break;
            case 'ArrowLeft':
            case 'Backspace':
            case 'PageUp':
                e.preventDefault();
                navigatePrev();
                break;
            case 's':
            case 'S':
                e.preventDefault();
                toggleNotes();
                break;
            case 'Home':
                e.preventDefault();
                showSlide(0);
                break;
            case 'End':
                e.preventDefault();
                showSlide(slides.length - 1);
                break;
        }
    });
    
    // Control buttons
    btnPrev.addEventListener('click', navigatePrev);
    btnNext.addEventListener('click', navigateNext);
    btnNotes.addEventListener('click', toggleNotes);
    
    // Window resize
    window.addEventListener('resize', scaleFrame);
    
    // Initial display
    showSlide(0);
});
