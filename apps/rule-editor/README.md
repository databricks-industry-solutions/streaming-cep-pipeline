# CEP Rules Editor (Databricks App)

Visual rule editor for the Streaming CEP Pipeline. Embeds the [GoRules JDM Editor](https://github.com/gorules/jdm-editor) and reads/writes rule JSON files in a Unity Catalog Volume so operators can change detection rules **without restarting the streaming pipeline** — the pipeline picks up changes on the next microbatch via `os.path.getmtime()` polling.

## Project Structure

```
apps/rule-editor/
├── app.py                 # FastAPI entrypoint (mounts API router + serves frontend dist)
├── app.yaml               # Databricks Apps config
├── apps-microbatch.py     # Streaming variant of S1 that loads rules from rules_apps Volume
├── requirements.txt
├── .databricksignore
├── backend/
│   ├── __init__.py
│   ├── api.py             # /api/rules CRUD against the Volume
│   └── config.py          # Settings (APP_VOLUME_PATH, APP_IS_LOCAL)
└── frontend/              # React + Vite + @gorules/jdm-editor
    ├── package.json
    ├── index.html
    ├── vite.config.ts
    └── src/
        ├── App.tsx
        ├── App.css
        ├── main.tsx
        └── api/client.ts
```

## Deployment

### 1. Prerequisites
- Databricks workspace with Unity Catalog
- Volume created: `/Volumes/cep_demo/network/rules_apps` (or override via `APP_VOLUME_PATH` in `app.yaml`)

### 2. Build the frontend
The FastAPI backend serves the React app from `frontend/dist`, so you must build it before deploying:

```bash
cd frontend
npm install
npm run build
```

### 3. Deploy the app
```bash
# from apps/rule-editor/
databricks apps create cep-rules-editor
databricks sync . /Workspace/Users/<you>/cep-rules-editor
databricks apps deploy cep-rules-editor --source-code-path /Workspace/Users/<you>/cep-rules-editor
```

Or via the workspace UI: Compute → Apps → Create App → point to the synced folder.

### 4. Verify
- Open the app URL — the editor UI should load
- Click `Open ▼ → Volume (Rules Apps)` — file list from the Volume appears
- Edit, then `Save` — file is written back to the Volume

## Local development

The Volume is not reachable from your laptop, so the backend falls back to a local `rules_mock/` directory when `APP_IS_LOCAL=true`.

```bash
# backend (terminal 1)
pip install -r requirements.txt
APP_IS_LOCAL=true python app.py
# → http://localhost:8000

# frontend (terminal 2)
cd frontend
npm install
npm run dev
# → http://localhost:5173 (proxies /api → :8000)
```
