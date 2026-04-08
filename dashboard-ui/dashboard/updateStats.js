const host = window.location.hostname || "localhost";

const PROCESSING_STATS_URL = `http://${host}:8100/stats`;
const ANALYZER_STATS_URL = `http://${host}:8110/stats`;
const HEALTH_URL = `http://${host}:8120/checks`;
const EVENT1_URL = `http://${host}:8110/todo/checklist-items`;
const EVENT2_URL = `http://${host}:8110/todo/checklist-summaries`;

function prettyPrint(data) {
  return JSON.stringify(data, null, 2);
}

async function getJSON(url) {
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`Request failed: ${url} (${response.status})`);
  }

  return await response.json();
}

function randomIndex(max) {
  if (max <= 0) return null;
  return Math.floor(Math.random() * max);
}

function setStatusText(elementId, value) {
  document.getElementById(elementId).textContent = value || "Unknown";
}

async function updateDashboard() {
  const statusMsg = document.getElementById("statusMsg");

  try {
    const healthData = await getJSON(HEALTH_URL);
    setStatusText("receiverStatus", healthData.receiver);
    setStatusText("storageStatus", healthData.storage);
    setStatusText("processingStatus", healthData.processing);
    setStatusText("analyzerStatus", healthData.analyzer);

    const processingStats = await getJSON(PROCESSING_STATS_URL);
    document.getElementById("processingStats").textContent = prettyPrint(processingStats);

    const analyzerStats = await getJSON(ANALYZER_STATS_URL);
    document.getElementById("analyzerStats").textContent = prettyPrint(analyzerStats);

    const event1Count = analyzerStats.num_event1 || 0;
    const event2Count = analyzerStats.num_event2 || 0;

    const event1Index = randomIndex(event1Count);
    const event2Index = randomIndex(event2Count);

    if (event1Index === null) {
      document.getElementById("event1").textContent = "No checklist item events yet.";
    } else {
      const event1 = await getJSON(`${EVENT1_URL}?index=${event1Index}`);
      document.getElementById("event1").textContent = prettyPrint(event1);
    }

    if (event2Index === null) {
      document.getElementById("event2").textContent = "No checklist summary events yet.";
    } else {
      const event2 = await getJSON(`${EVENT2_URL}?index=${event2Index}`);
      document.getElementById("event2").textContent = prettyPrint(event2);
    }

    document.getElementById("lastUpdated").textContent =
      healthData.last_update ||
      processingStats.last_updated ||
      analyzerStats.last_updated ||
      new Date().toLocaleString();

    statusMsg.textContent = "Dashboard updated successfully.";
  } catch (error) {
    statusMsg.textContent = `Error: ${error.message}`;
  }
}

updateDashboard();
setInterval(updateDashboard, 5000);