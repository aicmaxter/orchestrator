# demarrer.ps1 -- lance l'orchestrateur local + l'agent netsec-agent en mode `work`.
#
# ASCII uniquement : Windows PowerShell 5.1 lit un .ps1 sans BOM en ANSI, donc
# un tiret cadratin ou un accent casse l'analyse du script (erreur de parsing).
#
# Pourquoi un script : lancer un process Node de longue duree depuis un shell
# MSYS/git-bash echoue ici ("stdin is not a tty") ; Start-Process detache
# proprement et redirige les journaux dans ~/orchestrator/logs et
# ~/agents/netsec-agent/logs.
#
#   powershell -ExecutionPolicy Bypass -File demarrer.ps1
#   powershell -ExecutionPolicy Bypass -File demarrer.ps1 -Arreter

param(
    [switch]$Arreter
)

$OrchDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$AgentDir = Join-Path $env:USERPROFILE "agents\netsec-agent"
$OrchLog  = Join-Path $OrchDir "logs"
$AgentLog = Join-Path $AgentDir "logs"
New-Item -ItemType Directory -Force -Path $OrchLog, $AgentLog | Out-Null

function Stop-Matching($name, $pattern) {
    Get-CimInstance Win32_Process -Filter "Name='$name'" |
        Where-Object { $_.CommandLine -like $pattern } |
        ForEach-Object {
            Stop-Process -Id $_.ProcessId -Force
            Write-Host ("  arret  {0} pid {1}" -f $name, $_.ProcessId)
        }
}

if ($Arreter) {
    Write-Host "Arret de la pile :"
    Stop-Matching "node.exe" "*agent.mjs*"
    Stop-Matching "python.exe" "*orchestrateur.py*"
    Stop-Matching "python.exe" "*builder_daemon.py*"
    Write-Host "Termine."
    exit 0
}

Write-Host "Arret des instances existantes..."
Stop-Matching "node.exe" "*agent.mjs*"
Stop-Matching "python.exe" "*orchestrateur.py*"
Stop-Matching "python.exe" "*builder_daemon.py*"
Start-Sleep -Seconds 2

Write-Host "Orchestrateur..."
$orch = Start-Process -FilePath "python" -ArgumentList "orchestrateur.py" -WorkingDirectory $OrchDir `
    -RedirectStandardOutput (Join-Path $OrchLog "server.log") `
    -RedirectStandardError  (Join-Path $OrchLog "server.err.log") `
    -WindowStyle Hidden -PassThru
Write-Host ("  pid {0} - http://127.0.0.1:8000/ (console), /docs (API)" -f $orch.Id)

$pret = $false
foreach ($i in 1..30) {
    Start-Sleep -Milliseconds 500
    try {
        Invoke-RestMethod -Uri "http://127.0.0.1:8000/health" -TimeoutSec 3 | Out-Null
        $pret = $true; break
    } catch { }
}
if (-not $pret) { Write-Warning "Pas de reponse de l'orchestrateur - voir logs\server.err.log"; exit 1 }
Write-Host "  /health repond ok"

$AgentsDir = Join-Path $env:USERPROFILE "agents"
$Agents = @("netsec-agent", "research-agent", "qa-agent")
foreach ($nom in $Agents) {
    $dossier = Join-Path $AgentsDir $nom
    if (-not (Test-Path $dossier)) { Write-Warning "Agent absent : $dossier"; continue }
    $logs = Join-Path $dossier "logs"
    New-Item -ItemType Directory -Force -Path $logs | Out-Null
    Write-Host ("Agent {0} (mode work)..." -f $nom)
    $agentJs = Join-Path $dossier "agent.mjs"
    $agent = Start-Process -FilePath "node" `
        -ArgumentList ('"{0}"' -f $agentJs), "work" `
        -WorkingDirectory $dossier `
        -RedirectStandardOutput (Join-Path $logs "work.log") `
        -RedirectStandardError  (Join-Path $logs "work.err.log") `
        -WindowStyle Hidden -PassThru
    Write-Host ("  pid {0} - journal {1}" -f $agent.Id, (Join-Path $logs "work.log"))
}

Write-Host ""
# builder-01 : present sur le reseau en permanence (builder.py ne s'inscrit que
# le temps d'un run, sinon l'agent apparait OFFLINE dans la console).
$BuilderDir = Join-Path $AgentsDir "builder-agent"
$DaemonJs = Join-Path $BuilderDir "builder_daemon.py"
if (Test-Path $DaemonJs) {
    Write-Host "Presence reseau de builder-01 (builder_daemon.py)..."
    $logs = Join-Path $BuilderDir "logs"
    New-Item -ItemType Directory -Force -Path $logs | Out-Null
    $daemon = Start-Process -FilePath "python" -ArgumentList "builder_daemon.py" -WorkingDirectory $BuilderDir `
        -RedirectStandardOutput (Join-Path $logs "daemon.log") `
        -RedirectStandardError  (Join-Path $logs "daemon.err.log") `
        -WindowStyle Hidden -PassThru
    Write-Host ("  pid {0} - journal {1}" -f $daemon.Id, (Join-Path $logs "daemon.log"))
} else {
    Write-Warning "builder_daemon.py introuvable : $BuilderDir"
}

Write-Host ""
Write-Host "Cle operateur :" (Get-Content (Join-Path $OrchDir ".keys.json") | ConvertFrom-Json).admin_key
Write-Host "Console       : http://127.0.0.1:8000/"
