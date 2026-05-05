import io
import json
import queue
import shutil
import tempfile
import threading
import zipfile
from pathlib import Path

import bcrypt
from fastapi import BackgroundTasks, Depends, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles

import language_learner as ll

app = FastAPI()
security = HTTPBasic()
DATA_ROOT = Path("data")
USERS_FILE = Path("users.json")
_run_lock = threading.Lock()


# ── Auth ──────────────────────────────────────────────────────────────────────

def _load_users() -> dict:
    return json.loads(USERS_FILE.read_text()) if USERS_FILE.exists() else {}


def _get_user(creds: HTTPBasicCredentials = Depends(security)) -> str:
    users = _load_users()
    pw_hash = users.get(creds.username, "")
    if not pw_hash or not bcrypt.checkpw(creds.password.encode(), pw_hash.encode()):
        raise HTTPException(status_code=401, headers={"WWW-Authenticate": "Basic"})
    return creds.username


def _user_path(username: str) -> Path:
    p = DATA_ROOT / username
    p.mkdir(parents=True, exist_ok=True)
    return p


# ── User config helpers ───────────────────────────────────────────────────────

def _load_user_config(username: str) -> dict:
    f = _user_path(username) / "config.json"
    return json.loads(f.read_text()) if f.exists() else {}


def _save_user_config(username: str, config: dict) -> None:
    (_user_path(username) / "config.json").write_text(json.dumps(config, indent=2))


# ── Misc helpers ──────────────────────────────────────────────────────────────

def _parse_day_spec(spec: str) -> list[int]:
    days: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            days.update(range(int(a.strip()), int(b.strip()) + 1))
        else:
            days.add(int(part))
    return sorted(days)


def _merged_templates(user_templates: dict) -> dict:
    """Merge default templates with user overrides, respecting disabled flags."""
    merged = {}
    for name, (pattern, reps, speed, output_type) in ll.Config.TEMPLATES.items():
        override = user_templates.get(name, {})
        if override.get("disabled"):
            continue
        merged[name] = (
            override.get("pattern", pattern),
            override.get("reps", reps),
            override.get("speed", speed),
            override.get("output_type", output_type),
        )
    for name, tdata in user_templates.items():
        if name not in ll.Config.TEMPLATES and not tdata.get("disabled"):
            merged[name] = (
                tdata["pattern"],
                tdata.get("reps", 1),
                tdata.get("speed", 1.0),
                tdata.get("output_type", "audio"),
            )
    return merged


app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def index():
    return FileResponse("static/index.html")


# ── CSV ───────────────────────────────────────────────────────────────────────

@app.post("/csv/upload")
async def upload_csv(
    file: UploadFile = File(...),
    mode: str = Form("replace"),
    username: str = Depends(_get_user),
):
    content = (await file.read()).decode("utf-8-sig")
    dest = _user_path(username) / "source.csv"
    if mode == "append" and dest.exists():
        existing = dest.read_text(encoding="utf-8-sig").rstrip()
        lines = content.splitlines()
        if lines and any(h in lines[0] for h in ("L1", "L2", "W1", "W2", "StudyDay")):
            content = "\n".join(lines[1:])
        dest.write_text(existing + "\n" + content.strip() + "\n", encoding="utf-8-sig")
    else:
        dest.write_text(content, encoding="utf-8-sig")
    lines = [l for l in dest.read_text(encoding="utf-8-sig").splitlines() if l.strip()]
    return {"status": "ok", "rows": max(0, len(lines) - 1)}


@app.get("/csv/info")
def csv_info(username: str = Depends(_get_user)):
    src = _user_path(username) / "source.csv"
    if not src.exists():
        return {"rows": 0}
    lines = [l for l in src.read_text(encoding="utf-8-sig").splitlines() if l.strip()]
    return {"rows": max(0, len(lines) - 1)}


# ── Config (language/voice) ───────────────────────────────────────────────────

@app.get("/config")
def get_config(username: str = Depends(_get_user)):
    cfg = _load_user_config(username)
    return {
        "base_lang":   cfg.get("base_lang",   ll.Config.BASE_LANG_CODE),
        "base_voice":  cfg.get("base_voice",  ll.Config.BASE_VOICE_NAME),
        "target_lang": cfg.get("target_lang", ll.Config.TARGET_LANG_CODE),
        "target_voice":cfg.get("target_voice",ll.Config.TARGET_VOICE_NAME),
    }


@app.post("/config")
def save_config(
    base_lang: str = Form(...),
    base_voice: str = Form(...),
    target_lang: str = Form(...),
    target_voice: str = Form(...),
    username: str = Depends(_get_user),
):
    cfg = _load_user_config(username)
    cfg.update({"base_lang": base_lang, "base_voice": base_voice,
                "target_lang": target_lang, "target_voice": target_voice})
    _save_user_config(username, cfg)
    return {"status": "ok"}


# ── Templates ─────────────────────────────────────────────────────────────────

@app.get("/templates")
def get_templates(username: str = Depends(_get_user)):
    user_templates = _load_user_config(username).get("templates", {})
    result = []
    for name, (pattern, reps, speed, output_type) in ll.Config.TEMPLATES.items():
        override = user_templates.get(name, {})
        result.append({
            "name":        name,
            "pattern":     override.get("pattern",     pattern),
            "reps":        override.get("reps",        reps),
            "speed":       override.get("speed",       speed),
            "output_type": override.get("output_type", output_type),
            "is_default":  True,
            "modified":    bool(override and "pattern" in override),
            "disabled":    override.get("disabled", False),
        })
    for name, tdata in user_templates.items():
        if name not in ll.Config.TEMPLATES:
            result.append({
                "name":        name,
                "pattern":     tdata.get("pattern", ""),
                "reps":        tdata.get("reps", 1),
                "speed":       tdata.get("speed", 1.0),
                "output_type": tdata.get("output_type", "audio"),
                "is_default":  False,
                "modified":    False,
                "disabled":    tdata.get("disabled", False),
            })
    return result


@app.post("/templates/save")
def save_template(
    name: str = Form(...),
    pattern: str = Form(...),
    reps: int = Form(...),
    speed: float = Form(...),
    output_type: str = Form(...),
    username: str = Depends(_get_user),
):
    if not name.strip():
        raise HTTPException(400, "Template name is required")
    cfg = _load_user_config(username)
    templates = cfg.setdefault("templates", {})
    existing = templates.get(name, {})
    existing.update({"pattern": pattern, "reps": reps, "speed": speed, "output_type": output_type})
    existing.pop("disabled", None)
    templates[name] = existing
    _save_user_config(username, cfg)
    return {"status": "ok"}


@app.post("/templates/toggle")
def toggle_template(
    name: str = Form(...),
    disabled: str = Form(...),
    username: str = Depends(_get_user),
):
    cfg = _load_user_config(username)
    templates = cfg.setdefault("templates", {})
    templates.setdefault(name, {})["disabled"] = disabled.lower() == "true"
    _save_user_config(username, cfg)
    return {"status": "ok"}


@app.post("/templates/reset")
def reset_template(name: str = Form(...), username: str = Depends(_get_user)):
    """Remove user override, reverting a default template to its original values."""
    if name not in ll.Config.TEMPLATES:
        raise HTTPException(400, "Only default templates can be reset")
    cfg = _load_user_config(username)
    cfg.get("templates", {}).pop(name, None)
    _save_user_config(username, cfg)
    return {"status": "ok"}


@app.post("/templates/delete")
def delete_template(name: str = Form(...), username: str = Depends(_get_user)):
    if name in ll.Config.TEMPLATES:
        raise HTTPException(400, "Cannot delete a built-in template — disable it instead")
    cfg = _load_user_config(username)
    cfg.get("templates", {}).pop(name, None)
    _save_user_config(username, cfg)
    return {"status": "ok"}


# ── Run ───────────────────────────────────────────────────────────────────────

@app.post("/run")
def run(
    mode: str = Form("sr"),
    audio_format: str = Form("mp3"),
    do_zip: str = Form("false"),
    username: str = Depends(_get_user),
):
    if not _run_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="A run is already in progress")

    user_path = _user_path(username)
    log_q: queue.Queue = queue.Queue()

    def _run():
        # Save original Config so we can restore after the run
        orig = {
            "SOURCE_FILE":      ll.Config.SOURCE_FILE,
            "OUTPUT_ROOT_DIR":  ll.Config.OUTPUT_ROOT_DIR,
            "TTS_CACHE_DIR":    ll.Config.TTS_CACHE_DIR,
            "BASE_LANG_CODE":   ll.Config.BASE_LANG_CODE,
            "BASE_VOICE_NAME":  ll.Config.BASE_VOICE_NAME,
            "TARGET_LANG_CODE": ll.Config.TARGET_LANG_CODE,
            "TARGET_VOICE_NAME":ll.Config.TARGET_VOICE_NAME,
            "TEMPLATES":        dict(ll.Config.TEMPLATES),
        }
        try:
            ll.Config.SOURCE_FILE     = user_path / "source.csv"
            ll.Config.OUTPUT_ROOT_DIR = user_path / "output"
            ll.Config.TTS_CACHE_DIR   = user_path / "tts_cache"

            cfg = _load_user_config(username)
            if cfg.get("base_lang"):    ll.Config.BASE_LANG_CODE   = cfg["base_lang"]
            if cfg.get("base_voice"):   ll.Config.BASE_VOICE_NAME  = cfg["base_voice"]
            if cfg.get("target_lang"):  ll.Config.TARGET_LANG_CODE = cfg["target_lang"]
            if cfg.get("target_voice"): ll.Config.TARGET_VOICE_NAME= cfg["target_voice"]

            user_templates = cfg.get("templates", {})
            if user_templates:
                ll.Config.TEMPLATES = _merged_templates(user_templates)

            ll.set_log_callback(lambda msg: log_q.put(msg))

            output_dir = user_path / "output"
            before = set(output_dir.rglob("*")) if output_dir.exists() else set()

            ll.main_workflow(ll.RunConfig(
                mode=mode,
                audio_format=audio_format,
                do_zip=do_zip.lower() == "true",
            ))

            after = set(output_dir.rglob("*")) if output_dir.exists() else set()
            new_files = sorted(f for f in (after - before) if f.is_file())
            if new_files:
                buf = io.BytesIO()
                with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
                    for f in new_files:
                        zf.write(f, f.relative_to(output_dir))
                (user_path / "latest_run.zip").write_bytes(buf.getvalue())
                log_q.put(f"📦 {len(new_files)} new file(s) ready to download")

        except ValueError as e:
            log_q.put(f"❌ {e}")
        except Exception as e:
            log_q.put(f"❌ Unexpected error: {e}")
        finally:
            for k, v in orig.items():
                setattr(ll.Config, k, v)
            log_q.put(None)
            _run_lock.release()

    threading.Thread(target=_run, daemon=True).start()

    def _stream():
        while True:
            msg = log_q.get()
            if msg is None:
                yield "data: [DONE]\n\n"
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")


# ── Downloads ─────────────────────────────────────────────────────────────────

@app.get("/download/info")
def download_info(username: str = Depends(_get_user)):
    return {"has_latest": (_user_path(username) / "latest_run.zip").exists()}


@app.get("/download/latest")
def download_latest(username: str = Depends(_get_user)):
    zip_path = _user_path(username) / "latest_run.zip"
    if not zip_path.exists():
        raise HTTPException(404, "No recent run found")
    return FileResponse(zip_path, media_type="application/zip",
                        headers={"Content-Disposition": "attachment; filename=latest_run.zip"})


@app.get("/download/all")
def download_all(background_tasks: BackgroundTasks, username: str = Depends(_get_user)):
    output = _user_path(username) / "output"
    files = sorted(f for f in output.rglob("*") if f.is_file()) if output.exists() else []
    if not files:
        raise HTTPException(404, "No output files yet")
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    tmp_path = Path(tmp.name)
    tmp.close()
    with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_STORED) as zf:
        for f in files:
            zf.write(f, f.relative_to(output))
    background_tasks.add_task(tmp_path.unlink, missing_ok=True)
    return FileResponse(str(tmp_path), media_type="application/zip",
                        headers={"Content-Disposition": "attachment; filename=all_days.zip"})


@app.post("/download/days")
def download_days(spec: str = Form(...), username: str = Depends(_get_user)):
    try:
        day_nums = _parse_day_spec(spec)
    except ValueError:
        raise HTTPException(400, "Invalid day specification")
    output = _user_path(username) / "output"
    files = []
    for d in day_nums:
        day_dir = output / f"day_{d:03d}"
        if day_dir.exists():
            files.extend(sorted(f for f in day_dir.iterdir() if f.is_file()))
    if not files:
        raise HTTPException(404, "No files found for those days")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
        for f in files:
            zf.write(f, f.relative_to(output))
    buf.seek(0)
    safe = spec.strip().replace(" ", "").replace(",", "_")
    return StreamingResponse(buf, media_type="application/zip",
                             headers={"Content-Disposition": f"attachment; filename=days_{safe}.zip"})


# ── Day management ────────────────────────────────────────────────────────────

@app.post("/days/delete")
def delete_days(spec: str = Form(...), username: str = Depends(_get_user)):
    try:
        day_nums = _parse_day_spec(spec)
    except ValueError:
        raise HTTPException(400, "Invalid day specification")
    output = _user_path(username) / "output"
    deleted = []
    for d in day_nums:
        day_dir = output / f"day_{d:03d}"
        if day_dir.exists():
            shutil.rmtree(day_dir)
            deleted.append(d)
    return {"deleted": deleted}


# ── File browser ──────────────────────────────────────────────────────────────

@app.get("/files")
def list_files(username: str = Depends(_get_user)):
    output = _user_path(username) / "output"
    if not output.exists():
        return {"days": []}
    days: dict = {}
    for f in sorted(output.rglob("*")):
        if f.is_file():
            day = f.parent.name
            days.setdefault(day, []).append(f.name)
    return {"days": [{"day": k, "files": v} for k, v in days.items()]}


@app.get("/files/{filepath:path}")
def download_file(filepath: str, username: str = Depends(_get_user)):
    output = (_user_path(username) / "output").resolve()
    target = (output / filepath).resolve()
    if not str(target).startswith(str(output)):
        raise HTTPException(status_code=403)
    if not target.exists():
        raise HTTPException(status_code=404)
    return FileResponse(target)
