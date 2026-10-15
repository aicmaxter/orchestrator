# Multi-Agent Orchestrator (local)

Service unique qui orchestre tes agents, avec **deux protocoles** pour rester
compatible avec les clients déjà écrits :

| Protocole | Clients | Endpoints |
|---|---|---|
| `multi-agent orchestrator` (Bearer) | `agents/netsec-agent` (Node, zéro dépendance) | `POST /api/v1/agents/enroll`, `GET /api/v1/agents/me`, `POST /api/v1/agents/heartbeat`, `GET /api/v1/agents/tasks`, `POST /api/v1/agents/tasks/{id}/result` |
| `réseau local d'agents` (X-API-Key) | `agents/builder-agent` (Python/httpx) | `POST /register`, `POST /delegate`, `GET /agents` |

Console d'administration (navigateur) : `GET /` — documentation d'API : `GET /docs`.

## Démarrage

```bash
cd ~/orchestrator
python -m pip install -r requirements.txt
python orchestrateur.py            # écoute sur http://127.0.0.1:8000
```

Au premier démarrage, deux clés sont générées et écrites dans `.keys.json`
(gitignoré) :

- `admin_key` — clé opérateur : console, création de tâches, `/register`, `/delegate` ;
- `enrollment_key` — clé d'enregistrement : `Authorization: Bearer <clé>` sur `POST /api/v1/agents/enroll`.

La clé d'enregistrement se lit aussi dans la console (section « Clé
d'enregistrement ») ou sur `GET /api/v1/settings/enrollment-key` — **chaque
consultation est journalisée**. `POST /api/v1/settings/rotate-enrollment-key`
la tourne : les agents déjà inscrits gardent leur jeton d'agent, seuls les
nouveaux enrôlements exigent la nouvelle clé.

## Variables

| Variable | Défaut | Rôle |
|---|---|---|
| `ORCH_HOST` / `ORCH_PORT` | `127.0.0.1` / `8000` | écoute |
| `ORCH_ADMIN_KEY` | générée | force la clé opérateur |
| `ORCH_ONLINE_WINDOW` | `90` | secondes sans battement → `OFFLINE` |
| `ORCH_LEASE` | `120` | durée d'un bail de tâche avant relivraison |
| `ORCH_DELEGATE_TIMEOUT` | `150` | attente max de `/delegate` |

## Fichiers

| Fichier | Rôle |
|---|---|
| `orchestrateur.py` | le service (FastAPI, SQLite) |
| `console.html` | console d'administration servie sur `/` |
| `aliases.json` | alias de capacités (ex. `security_analysis` → `vulnerability_assessment`) |
| `data/orchestrateur.db` | agents, jetons (hachés), tâches, journal, réglages |
| `.keys.json` | clés générées — **ne pas committer** |

## Parc des agents

| Agent | Protocole | Capacités | Actions réelles |
|---|---|---|---|
| `netsec-agent` (Node) | v1 | network_security, cybersecurity, vulnerability_assessment, threat_detection, incident_response, network_hardening | `system_report`, `security_audit`, `network_inventory` (lecture seule) |
| `research-agent` (Node) | v1 | research, veille, sources | `brief` sourcé (Wikipédia, arXiv, Hacker News), `sources_check`, `fetch_page` |
| `qa-agent` (Node) | v1 | testing, quality_assurance, qa, link_check | `html_check`, `link_check`, `smoke_http`, `test_plan` |
| `builder-agent` (Python, LAN) | /register + /delegate | web_development, application_development, frontend, backend | `builder.py` délègue research → security_analysis → testing puis écrit la page |

Mécanique commune des agents Node : `~/agents/agent-kit` (voir son README).
Alias de capacités : `aliases.json` (`security_analysis` → `vulnerability_assessment`
/ `cybersecurity`, `pentest` → `vulnerability_assessment`).

## Chaîne LAB 3 prouvée de bout en bout (15/10/2026)

```
python builder.py --objective "Prépare une landing page sur les agents autonomes."
[REGISTER  ] builder-01 présenté au réseau
[DELEGATE  ] research          → research-agent  (2 938 car.)   28 s
[DELEGATE  ] security_analysis → netsec-agent    (1 773 car.)   56 s
[DELEGATE  ] testing           → qa-agent        (556 car.)      8 s
[BUILD     ] landing.html écrit (4 985 octets) à partir de ces résultats réels
```

Puis contrôle du livrable final : `/delegate testing html_check` →
`status=completed`, `verdict PASS` sur `~/agents/builder-agent/landing.html`
(5 133 octets, `lang="fr"`, `<h1>` unique, viewport présent, aucun script).

## Démarrer la pile

```powershell
cd ~/orchestrator
pip install -r requirements.txt
powershell -ExecutionPolicy Bypass -File demarrer.ps1     # orchestrateur + agent netsec-agent en mode work
python orchestrateur.py                                   # ou : uniquement l'orchestrateur
```

`demarrer.ps1 -Arreter` coupe les deux processus. Le script existe parce que
lancer un Node de longue durée depuis git-bash échoue ici (`stdin is not a
tty`) : `Start-Process` détache proprement et redirige les journaux dans
`logs/`.

À lancer depuis une **fenêtre PowerShell normale** : sous un shell dont la
sortie est redirigée vers un pipe (script d'automatisation, CI), l'appelant
attend la fermeture des handles héritées par les processus détachés — le script
a fini son travail, mais le pipe ne se ferme pas. Dans ce cas lance plutôt
`python orchestrateur.py` en avant-plan, ou `node agent.mjs work` dans une
seconde fenêtre.

Le poste de travail côté agent : `~/agents/netsec-agent` → `node agent.mjs work`
(heartbeat + exécution des tâches + remontée du résultat ; actions en lecture
seule documentées dans `WORK.md`). Sans mode `work`, un agent reste `ONLINE`
mais ne rend **aucun** résultat : la tâche finit en bail expiré et est relivrée.

## Preuves vérifiées (15/10/2026, sur ce poste)

| Vérification | Résultat |
|---|---|
| `GET /health` | `200 {"status":"ok","service":"multi-agent-orchestrator","version":"1.0.0"}` |
| Enrôlement sans clé / clé fausse | `401 UNAUTHENTICATED` — « Clé d'enregistrement requise. » / « … invalide. » |
| Enrôlement avec clé | `201` + jeton d'agent ; `GET /api/v1/agents/me` renvoie identité, 6 capacités, `tasks_done`/`tasks_failed` |
| Chaîne complète (netsec-agent) | tâche `tsk_9d72aad3a2c0` → routée → livrée → **exécutée en 31 s** → `completed`, résultat de 1 767 caractères stocké |
| Délégation LAN (`/delegate`) | `security_analysis` → *alias* → `netsec-agent` → `status=completed` en 24,8 s, résultat réel rendu à l'appelant |
| Protocole LAN (`/register`) | `Builder Agent` enregistré (4 capacités), visible dans `GET /agents` |
| Capacité sans agent | tâche laissée `unrouted` avec la raison exacte ; `/delegate` répond `refused` |
| Jeton d'agent bidon | `401` sur `POST /api/v1/agents/heartbeat` |
| Rotation de clé | journalisée (`KEY_READ`, `KEY_ROTATED`) ; l'agent déjà inscrit reste `ONLINE` |
| Console (navigateur) | `GET /` rend agents (état, « vu il y a N s », capacités), journal et tâches |

## Cycle de vie d'une tâche

```
created → unrouted (aucun agent)  |  queued → leased (livrée) → running (côté agent)
        → completed | failed
```

Routage : premier agent `ONLINE` (battement < 90 s) qui déclare la capacité,
sinon alias de `aliases.json`, sinon tâche laissée `unrouted` avec la raison
exacte. Le bail expire au bout de `ORCH_LEASE` secondes : une tâche non rendue
est relivrée au cycle suivant.

## Sécurité

- Jetons d'agent stockés **hachés** (SHA-256), jamais en clair ; les clés ne
  sortent que par `.keys.json` (local) et la console.
- `X-API-Key` / `Bearer` comparés en temps constant (`secrets.compare_digest`).
- Toute action sensible (enrôlement, routage, délégation, rotation, lecture de
  clé) écrit une ligne dans `events` — lisible sur `GET /api/v1/events`.
- Un agent ne peut rendre un résultat que pour **ses** tâches (403 sinon).
