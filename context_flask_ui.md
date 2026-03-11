# Flask UI Context

## Architecture overview

The dashboard is a Flask app (`app.py`) serving a single Jinja2 template (`templates/index.html`). There is no JavaScript framework — the UI is server-rendered HTML with vanilla JS for interactivity.

The pipeline runner (`worker.py`) is a separate Python process used for headless deployment. In production, both processes start together via `deploy.sh`.

```
deploy.sh
  ├── python worker.py &      ← CH sweep runs headlessly, exits when done
  └── exec python app.py      ← Flask stays running, serves dashboard
```

In development, Flask and worker run in separate Replit workflows.

---

## File responsibilities

| File | Role |
|---|---|
| `app.py` | Flask server — API endpoints, pipeline process management, CSV loading |
| `templates/index.html` | Single-page dashboard — all HTML, CSS, and JS in one file |
| `worker.py` | Headless entry point for CH sweep — calls `run_office_pipeline()` with pre-baked args |
| `deploy.sh` | Production launch script — starts worker in background, then Flask |

---

## CSV files the dashboard reads

| Variable | File | Contains |
|---|---|---|
| `ENRICHED_CSV` | `unit8_leads_enriched.csv` | Unit 8 wellness/clinical leads (1,272 leads, **never touch**) |
| `EXCLUDED_CSV` | `unit8_leads_excluded.csv` | Unit 8 leads excluded during refinement |
| `OFFICE_CSV` | `office_leads.csv` | Office occupier leads from CH sweep |
| `CSV_FILE` | `leads.csv` | Legacy file (not used in current dashboard views) |
| `OPENAI_COST_FILE` | `/tmp/openai_enrichment_cost.json` | Daily OpenAI spend, written by enricher |

---

## All API endpoints

### Data endpoints

| Endpoint | Method | Description |
|---|---|---|
| `GET /` | GET | Renders the full dashboard. Loads all CSVs, computes stats, passes to template. Office leads with `geo_relevance == 'exclude'` are filtered out of the template context here. |
| `GET /api/leads` | GET | JSON list of leads. Params: `category` (unit8/office/all), `min_score`, `search` (fuzzy text match on company/sector/location), `geo` (local/review/exclude/local_review). Used by the filter/search toolbar. Returns `{leads: [...], stats: {...}}`. |
| `GET /api/stats` | GET | JSON stats for all three categories (unit8, office, total) plus OpenAI cost. Used by the 60-second auto-refresh. Returns `{total: {...}, unit8: {...}, office: {...}, openai: {...}}`. |
| `GET /api/refresh` | GET | Returns full unit8 and office lead lists plus stats. Not currently used by the UI (was used before auto-refresh was added). |
| `GET /api/refinement-stats` | GET | Stats from `unit8_leads_enriched.csv` and `unit8_leads_excluded.csv` for the download panel (e.g. high/medium/low data score counts, CH directors, contacts, email types). |

### Download endpoints

| Endpoint | Method | Description |
|---|---|---|
| `GET /api/download/<category>` | GET | Downloads a filtered CSV. Category: `unit8`, `office`, or `all`. Optional `?min_score=N` filter. The downloaded CSV uses a fixed fieldnames list defined in `app.py` (not all BusinessLead fields — only the ones relevant for outreach). |
| `GET /api/download/enriched` | GET | Streams `unit8_leads_enriched.csv` directly as a file download. |
| `GET /api/download/excluded` | GET | Streams `unit8_leads_excluded.csv` directly as a file download. |

**Download CSV fieldnames** (in `app.py`'s `download_csv()`) — these are curated for outreach use:
```
company_name, website, website_verified, facebook_url,
contact_name, contact_names, contact_email, personal_email_guesses,
team_email_guesses, principal_name, principal_email_guess,
generic_email, email_type, name_review_needed, missing_email,
data_score, confidence_score, sector, location, phone, linkedin,
ai_score, ai_reason, tag, google_rating, category, place_id,
search_town, enrichment_source, enrichment_status,
enrichment_attempts, refinement_notes,
geo_relevance, date_of_creation, size_signal
```

### Pipeline control endpoints

| Endpoint | Method | Description |
|---|---|---|
| `POST /api/pipeline/start/<key>` | POST | Starts a pipeline subprocess. Key must be `office`, `unit8`, or `office_enrich`. Returns 409 if already running. Sets up a state dict in `_pipeline_state` then spawns a daemon thread calling `_run_pipeline(key)`. |
| `POST /api/pipeline/stop/<key>` | POST | Sends SIGTERM to the pipeline subprocess. Waits 10 seconds, then SIGKILL if still alive. |
| `GET /api/pipeline/status` | GET | Returns status for all three pipelines: `{status: idle/running/finished/stopped/error, elapsed: N, duration: N, exit_code: N, label: str}`. Polled every 3 seconds by the UI while any pipeline is running. |
| `GET /api/pipeline/log/<key>` | GET | Returns last N lines of `/tmp/pipeline_{key}.log`. Default tail: 30 lines, max 200. The log file is written by `_run_pipeline` as the subprocess outputs to stdout. |

---

## Pipeline process management

Pipelines are defined in the `PIPELINES` dict:

```python
PIPELINES = {
    'office':        {'cmd': ['python', 'main.py', '--mode', 'office'],    'label': 'Office Lead Search'},
    'unit8':         {'cmd': ['python', 'main.py', '--wellness'],           'label': 'Unit 8 Wellness Search'},
    'office_enrich': {'cmd': ['python', 'run_office_enrichment.py'],        'label': 'Office Email Enrichment'},
}
```

`_pipeline_state` is a dict keyed by pipeline key, protected by `_pipeline_lock` (a `threading.Lock`). Each state entry contains:
- `phase`: `starting` → `running` → `finished` / `stopped`
- `started`: Unix timestamp
- `pid`: subprocess PID
- `proc`: `subprocess.Popen` object
- `log`: path to log file
- `finished`: Unix timestamp (set on completion)
- `exit_code`: process return code

`_run_pipeline(key)` runs in a daemon thread. It:
1. Spawns the subprocess with `stderr=STDOUT` so all output is merged
2. Writes each line to `/tmp/pipeline_{key}.log`
3. Also writes each line to Flask's `sys.stdout` with a `[OFFICE]`/`[UNIT8]`/`[OFFICE_ENRICH]` prefix (visible in Replit console)
4. Waits for subprocess to finish, then updates state

The subprocess runs in `cwd` = the project root (same as `app.py`'s directory). Environment variables are inherited from Flask's process, so all API keys are available.

---

## The `get_stats()` function

Takes a list of lead dicts and returns a stats dict. Key fields and their sources:

| Stat field | Source in CSV |
|---|---|
| `total` | `len(leads)` |
| `with_email` | `email` or `contact_email` is non-empty |
| `with_contact` | `contact_name` is non-empty |
| `enriched_complete` | `enrichment_status == 'complete'` |
| `email_guessed` | `email_guessed == 'true'` (lowercase) |
| `contact_verified` | `contact_verified == 'true'` (lowercase) |
| `ai_enriched` | `ai_enriched == 'true'` (lowercase) |
| `name_review` | `name_review_needed == 'True'` (**capitalised**) |
| `missing_email` | `missing_email == 'True'` (**capitalised**) |
| `geo_local/review/exclude` | `geo_relevance == 'local'/'review'/'exclude'` |
| `pct_complete` | `enriched_complete / total * 100` |
| `avg_confidence` | Average of integer `confidence_score` values |

**Boolean inconsistency**: `name_review_needed` and `missing_email` are checked with `== 'True'` (capital T), while most other boolean fields are checked with `== 'true'` (lowercase). The office pipeline writes lowercase booleans, which means the `name_review` and `missing_email` counts in the office tab stats will be 0 even when those fields are set. This is a known inconsistency — do not fix one side without fixing the other.

---

## Auto-refresh behaviour

**Stats-only refresh** runs every 60 seconds via `setInterval(refreshStats, 60000)`:
- Calls `GET /api/stats`
- Updates the `data-stat` attribute elements in the Unit 8 and Office stat grids
- Updates the tab button labels (e.g. "Unit 8 Occupiers (1272)")
- Updates the "Last updated: HH:MM:SS" timestamp in the header

**The table data does NOT auto-refresh** — it only updates when:
- The user manually clicks Filter or Reset
- A pipeline finishes and the log is toggled (which triggers a log fetch but not a table refresh)
- The user reloads the page

**Pipeline status polling** runs every 3 seconds via `startStatusPolling()`, but only while at least one pipeline is running. When all pipelines are idle/finished, `_statusInterval` is cleared. Polling restarts automatically when a new pipeline is started.

---

## Pipeline control cards (UI)

Each pipeline has a card in the "Search Pipelines" panel with:
- Title and description
- Status text (Ready / Running Xm Ys / Finished / Stopped / Error)
- Run button (green, `btn-run`)
- Stop button (red, `btn-stop`, hidden unless running)
- View Log button (purple, opens/closes a `<div class="log-viewer">`)

Button labels are set in a `labels` dict in both `startPipeline()` and `updatePipelineUI()`:
```js
const labels = {
  'office': 'Run Office Search',
  'unit8': 'Run Wellness Search',
  'office_enrich': 'Run Email Enrichment'
};
```

This dict must exist in both functions — it was previously missing from the error path, causing the button to show the raw key name on failure.

The log viewer shows the last 100 lines of the log file, auto-scrolls to bottom, and auto-refreshes while its pipeline is running (checked in `refreshLogIfVisible` called from `pollStatus`).

---

## Geo filtering in the UI

**At template render time** (`index()` route): office leads with `geo_relevance == 'exclude'` are silently removed from `office_leads` before passing to the template. This means excluded leads never appear in the initial page load.

**In the Office tab toolbar**, a `<select id="office-geo">` defaults to `"local_review"`. The `filterLeads('office')` function passes this as `?geo=local_review` to `/api/leads`, which then filters to only leads where `geo_relevance != 'exclude'`. Other options: `"local"`, `"review"`, `"exclude"` (to explicitly view excluded leads).

**Flag display in table**: Office tab rows show a coloured flag badge:
- `local` → green "local"
- `review` → amber "review" (with `refinement_notes` as tooltip)  
- `exclude` → red "exclude" (with `refinement_notes` as tooltip)

---

## `worker.py` — headless CH sweep

`worker.py` is a thin wrapper around `run_office_pipeline()`. It constructs an `argparse.Namespace` object with hardcoded defaults (same as calling `python main.py --mode office` with no extra flags) and calls the function directly.

Key hardcoded settings:
- `fresh=False` — never deletes existing CSV; always appends/deduplicates
- `dry_run=False` — always saves
- `no_enrich=False` — enrichment enabled
- `verbose=False` — silent mode

On `KeyboardInterrupt`, it exits cleanly with a message confirming that checkpoint saves preserve all progress. On any other exception, it prints a full traceback and exits with code 1.

---

## `deploy.sh` — production launch

```bash
python worker.py &         # background: CH sweep, exits when done
exec python app.py         # foreground: Flask, keeps process alive
```

`exec` replaces the shell process with Flask, so Flask's PID becomes the main process that Replit's VM deployment monitors. The worker exits naturally after the sweep completes. Flask stays alive indefinitely to serve the dashboard.

In development, worker and Flask run as separate Replit workflows, not via `deploy.sh`.

---

## Cache control headers

Every Flask response includes:
```
Cache-Control: no-cache, no-store, must-revalidate
Pragma: no-cache
Expires: 0
X-Frame-Options: ALLOWALL
```

`ALLOWALL` on `X-Frame-Options` is required because the Replit preview pane embeds the app in an iframe. Without it, the browser refuses to display the app in the preview.

---

## Things not to change

- **`unit8_leads_enriched.csv` is the source for the Unit 8 tab** — the enriched CSV is the final, curated dataset. Do not change `ENRICHED_CSV` to point at the raw `leads.csv`.
- **The `geo_relevance == 'exclude'` filter at the `index()` route** must stay — excluded leads should be hidden by default.
- **`X-Frame-Options: ALLOWALL`** — removing this breaks the Replit preview pane.
- **`_pipeline_lock`** — all reads and writes to `_pipeline_state` must be within `with _pipeline_lock`. The pipeline runs in a daemon thread and Flask handles requests in the main thread concurrently.
- **`daemon=True` on pipeline threads** — ensures threads don't prevent Flask from shutting down cleanly.
- **The `labels` dict in JS** must have entries for all three keys (`office`, `unit8`, `office_enrich`) in both `startPipeline` and `updatePipelineUI`. Missing entries cause the button to show the raw dict key on error.
