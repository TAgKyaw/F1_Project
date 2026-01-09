const API_URL = "http://127.0.0.1:8000";

// --- DOM ELEMENTS ---
const chatWindow = document.getElementById('chat-window');
const chatInput = document.querySelector('#user-input');
const sendBtn = document.querySelector('#send-btn');
const seasonYearEl = document.getElementById('season-year');
const totalRacesEl = document.getElementById('total-races');
const seasonLeaderEl = document.getElementById('season-leader');

// --- INITIALIZATION ---
document.addEventListener('DOMContentLoaded', () => {
    fetchOverview();
    initCharts();
});

// --- API ACTIONS ---
async function fetchOverview() {
    try {
        const res = await fetch(`${API_URL}/stats/overview`);
        const data = await res.json();
        seasonYearEl.innerText = data.season_year;
        totalRacesEl.innerText = data.races_completed;
        seasonLeaderEl.innerText = `${data.leader} (${data.leader_points}pts)`;
    } catch (err) {
        console.error("API Error", err);
        seasonLeaderEl.innerText = "Offline";
    }
}

async function initCharts() {
    // Consistency
    const resCons = await fetch(`${API_URL}/stats/consistency`);
    const dataCons = await resCons.json();
    if (dataCons.data) {
        new Chart(document.getElementById('consistencyChart'), {
            type: 'bar',
            data: {
                labels: Object.keys(dataCons.data),
                datasets: [{
                    label: 'Consistency (Lower=Better)',
                    data: Object.values(dataCons.data),
                    backgroundColor: 'rgba(254, 202, 87, 0.7)',
                    borderColor: '#feca57',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                plugins: { legend: { display: false } },
                scales: { y: { beginAtZero: true, grid: { color: 'rgba(255,255,255,0.1)' } } }
            }
        });
    }

    // Reliability
    const resRel = await fetch(`${API_URL}/stats/reliability`);
    const dataRel = await resRel.json();
    if (dataRel.data) {
        new Chart(document.getElementById('reliabilityChart'), {
            type: 'bar',
            data: {
                labels: Object.keys(dataRel.data),
                datasets: [{
                    label: 'Unreliability (Higher=Worse)',
                    data: Object.values(dataRel.data),
                    backgroundColor: 'rgba(255, 107, 129, 0.7)',
                    borderColor: '#ff6b81',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                plugins: { legend: { display: false } },
                scales: { y: { beginAtZero: true, grid: { color: 'rgba(255,255,255,0.1)' } } }
            }
        });
    }
}

// --- CHAT LOGIC ---
sendBtn.addEventListener('click', sendMessage);
chatInput.addEventListener('keypress', (e) => { if (e.key === 'Enter') sendMessage(); });

async function sendMessage() {
    const query = chatInput.value.trim();
    if (!query) return;

    addBubble(query, 'human');
    chatInput.value = '';

    try {
        const res = await fetch(`${API_URL}/chat`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query: query })
        });
        const response = await res.json();
        addBotResponse(response);
    } catch (err) {
        addBubble("Error: Turn on the API server!", 'bot');
    }
}

function addBubble(text, sender) {
    const msgDiv = document.createElement('div');
    msgDiv.className = `message ${sender}`;
    msgDiv.innerHTML = `<div class="bubble">${text.replace(/\n/g, '<br>')}</div>`;
    chatWindow.appendChild(msgDiv);
    chatWindow.scrollTop = chatWindow.scrollHeight;
}

function addBotResponse(response) {
    const msgDiv = document.createElement('div');
    msgDiv.className = `message bot`;
    let content = response.text.replace(/\n/g, '<br>');

    if (response.type === 'table' || response.type === 'kv_pairs') {
        let rows = '';
        for (const [k, v] of Object.entries(response.data)) {
             rows += `<tr><td>${k}</td><td>${Number(v).toFixed ? (Number(v)%1===0 ? v : Number(v).toFixed(2)) : v}</td></tr>`;
        }
        content += `<table class="chat-table">${rows}</table>`;
    }
    
    if (response.type === 'prediction') {
        content = `<strong>🔮 Prediction</strong><br>Driver: ${response.data.driver}<br>` +
                  `<div style="font-size:1.5rem;color:#feca57;font-weight:bold;margin:10px 0;">P${Math.round(response.data.predicted_position)}</div>` +
                  `<small>Confidence: High</small>`;
    }

    msgDiv.innerHTML = `<div class="bubble">${content}</div>`;
    chatWindow.appendChild(msgDiv);
    chatWindow.scrollTop = chatWindow.scrollHeight;
}
