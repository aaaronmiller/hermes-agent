"""Cron job storage and management."""
import copy
import json
import logging
import tempfile
import os
import re
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any

logger = logging.getLogger(__name__)

from hermes_time import now as _hermes_now

try:
    from croniter import croniter
    HAS_CRONITER = True
except ImportError:
    HAS_CRONITER = False

HERMES_DIR = Path(os.getenv("HERMES_HOME", Path.home() / ".hermes"))
CRON_DIR = HERMES_DIR / "cron"
JOBS_FILE = CRON_DIR / "jobs.json"
OUTPUT_DIR = CRON_DIR / "output"
ONESHOT_GRACE_SECONDS = 120


def _normalize_skill_list(skill: Optional[str] = None, skills: Optional[Any] = None) -> List[str]:
    if skills is None:
        raw_items = [skill] if skill else []
    elif isinstance(skills, str):
        raw_items = [skills]
    else:
        raw_items = list(skills)
    normalized: List[str] = []
    for item in raw_items:
        text = str(item or "").strip()
        if text and text not in normalized:
            normalized.append(text)
    return normalized


def _apply_skill_fields(job: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(job)
    skills = _normalize_skill_list(normalized.get("skill"), normalized.get("skills"))
    normalized["skills"] = skills
    normalized["skill"] = skills[0] if skills else None
    return normalized


def _secure_dir(path: Path):
    try:
        os.chmod(path, 0o700)
    except (OSError, NotImplementedError):
        pass


def _secure_file(path: Path):
    if path.exists():
        try:
            os.chmod(path, 0o600)
        except (OSError, NotImplementedError):
            pass


def ensure_dirs():
    CRON_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    _secure_dir(CRON_DIR)
    _secure_dir(OUTPUT_DIR)


def parse_duration(s: str) -> int:
    s = s.strip().lower()
    match = re.match(r'^(\d+)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)$', s)
    if not match:
        raise ValueError(f"Invalid duration: '{s}'. Use format like '30m', '2h', or '1d'")
    value = int(match.group(1))
    unit = match.group(2)[0]
    multipliers = {'m': 1, 'h': 60, 'd': 1440}
    return value * multipliers[unit]


def parse_schedule(schedule: str) -> Dict[str, Any]:
    schedule = schedule.strip()
    original = schedule
    schedule_lower = schedule.lower()
    if schedule_lower.startswith("every "):
        duration_str = schedule[6:].strip()
        minutes = parse_duration(duration_str)
        return {
            "kind": "interval",
            "minutes": minutes,
            "display": f"every {minutes}m"
        }
    parts = schedule.split()
    if len(parts) >= 5 and all(re.match(r'^[\d\*,\-/]+$', p) for p in parts[:5]):
        if not HAS_CRONITER:
            raise ValueError("Cron expressions require 'croniter' package. Install with: pip install croniter")
        try:
            croniter(schedule)
        except Exception as e:
            raise ValueError(f"Invalid cron expression '{schedule}': {e}")
        return {
            "kind": "cron",
            "expr": schedule,
            "display": schedule
        }
    if 'T' in schedule or re.match(r'^\d{4}-\d{2}-\d{2}', schedule):
        try:
            dt = datetime.fromisoformat(schedule.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.astimezone()
            return {
                "kind": "once",
                "run_at": dt.isoformat(),
                "display": f"once at {dt.strftime('%Y-%m-%d %H:%M')}"
            }
        except ValueError as e:
            raise ValueError(f"Invalid timestamp '{schedule}': {e}")
    try:
        minutes = parse_duration(schedule)
        run_at = _hermes_now() + timedelta(minutes=minutes)
        return {
            "kind": "once",
            "run_at": run_at.isoformat(),
            "display": f"once in {original}"
        }
    except ValueError:
        pass
    raise ValueError(
        f"Invalid schedule '{original}'. Use:\n"
        f"  - Duration: '30m', '2h', '1d' (one-shot)\n"
        f"  - Interval: 'every 30m', 'every 2h' (recurring)\n"
        f"  - Cron: '0 9 * * *' (cron expression)\n"
        f"  - Timestamp: '2026-02-03T14:00:00' (one-shot at time)"
    )


def _ensure_aware(dt: datetime) -> datetime:
    target_tz = _hermes_now().tzinfo
    if dt.tzinfo is None:
        local_tz = datetime.now().astimezone().tzinfo
        return dt.replace(tzinfo=local_tz).astimezone(target_tz)
    return dt.astimezone(target_tz)


def _recoverable_oneshot_run_at(schedule: Dict[str, Any], now: datetime, *, last_run_at: Optional[str] = None) -> Optional[str]:
    if schedule.get("kind") != "once":
        return None
    if last_run_at:
        return None
    run_at = schedule.get("run_at")
    if not run_at:
        return None
    run_at_dt = _ensure_aware(datetime.fromisoformat(run_at))
    if run_at_dt >= now - timedelta(seconds=ONESHOT_GRACE_SECONDS):
        return run_at
    return None


def _compute_grace_seconds(schedule: dict) -> int:
    MIN_GRACE = 120
    MAX_GRACE = 7200
    kind = schedule.get("kind")
    if kind == "interval":
        period_seconds = schedule.get("minutes", 1) * 60
        grace = period_seconds // 2
        return max(MIN_GRACE, min(grace, MAX_GRACE))
    if kind == "cron" and HAS_CRONITER:
        try:
            now = _hermes_now()
            cron = croniter(schedule["expr"], now)
            first = cron.get_next(datetime)
            second = cron.get_next(datetime)
            period_seconds = int((second - first).total_seconds())
            grace = period_seconds // 2
            return max(MIN_GRACE, min(grace, MAX_GRACE))
        except Exception:
            pass
    return MIN_GRACE


def compute_next_run(schedule: Dict[str, Any], last_run_at: Optional[str] = None) -> Optional[str]:
    now = _hermes_now()
    if schedule["kind"] == "once":
        return _recoverable_oneshot_run_at(schedule, now, last_run_at=last_run_at)
    elif schedule["kind"] == "interval":
        minutes = schedule["minutes"]
        if last_run_at:
            last = _ensure_aware(datetime.fromisoformat(last_run_at))
            next_run = last + timedelta(minutes=minutes)
        else:
            next_run = now + timedelta(minutes=minutes)
        return next_run.isoformat()
    elif schedule["kind"] == "cron":
        if not HAS_CRONITER:
            return None
        cron = croniter(schedule["expr"], now)
        next_run = cron.get_next(datetime)
        return next_run.isoformat()
    return None


def load_jobs() -> List[Dict[str, Any]]:
    ensure_dirs()
    if not JOBS_FILE.exists():
        return []
    try:
        with open(JOBS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get("jobs", [])
    except (json.JSONDecodeError, IOError):
        return []


def save_jobs(jobs: List[Dict[str, Any]]):
    ensure_dirs()
    fd, tmp_path = tempfile.mkstemp(dir=str(JOBS_FILE.parent), suffix='.tmp', prefix='.jobs_')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump({"jobs": jobs, "updated_at": _hermes_now().isoformat()}, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, JOBS_FILE)
        _secure_file(JOBS_FILE)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def create_job(
    prompt: str,
    schedule: Optional[str] = None,
    name: Optional[str] = None,
    repeat: Optional[int] = None,
    deliver: Optional[str] = None,
    origin: Optional[Dict[str, Any]] = None,
    skill: Optional[str] = None,
    skills: Optional[List[str]] = None,
    model: Optional[str] = None,
    provider: Optional[str] = None,
    base_url: Optional[str] = None,
    heartbeat: Optional[dict] = None,
    heartbeat_interval_seconds: Optional[int] = None,
) -> Dict[str, Any]:
    """Create a new cron job."""
    parsed_schedule = parse_schedule(schedule) if schedule is not None else None

    if repeat is not None and repeat <= 0:
        repeat = None
    if parsed_schedule and parsed_schedule.get("kind") == "once" and repeat is None:
        repeat = 1
    if deliver is None:
        deliver = "origin" if origin else "local"
    job_id = uuid.uuid4().hex[:12]
    now = _hermes_now().isoformat()
    normalized_skills = _normalize_skill_list(skill, skills)
    normalized_model = str(model).strip() if isinstance(model, str) else None
    normalized_provider = str(provider).strip() if isinstance(provider, str) else None
    normalized_base_url = str(base_url).strip().rstrip("/") if isinstance(base_url, str) else None
    label_source = (prompt or (normalized_skills[0] if normalized_skills else None)) or "cron job"

    job: Dict[str, Any] = {
        "id": job_id,
        "name": name or label_source[:50].strip(),
        "prompt": prompt,
        "skills": normalized_skills,
        "skill": normalized_skills[0] if normalized_skills else None,
        "model": normalized_model,
        "provider": normalized_provider,
        "base_url": normalized_base_url,
        "schedule": parsed_schedule if parsed_schedule else {},
        "schedule_display": parsed_schedule.get("display", schedule) if parsed_schedule else f"every {heartbeat_interval_seconds}s" if heartbeat_interval_seconds else "N/A",
        "repeat": {"times": repeat, "completed": 0},
        "enabled": True,
        "state": "scheduled",
        "paused_at": None,
        "paused_reason": None,
        "created_at": now,
        "last_run_at": None,
        "last_status": None,
        "last_error": None,
        "deliver": deliver,
        "origin": origin,
    }

    # Set next_run_at based on either schedule or heartbeat interval
    if heartbeat_interval_seconds is not None:
        job["heartbeat_interval_seconds"] = heartbeat_interval_seconds
        job["next_run_at"] = (_hermes_now() + timedelta(seconds=heartbeat_interval_seconds)).isoformat()
    else:
        job["next_run_at"] = compute_next_run(parsed_schedule) if parsed_schedule else None

    # Merge heartbeat config if provided
    if heartbeat:
        from cron.heartbeat import configure_job_for_heartbeat
        job = configure_job_for_heartbeat(job, heartbeat)

    jobs = load_jobs()
    jobs.append(job)
    save_jobs(jobs)
    return job


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    jobs = load_jobs()
    for job in jobs:
        if job["id"] == job_id:
            return _apply_skill_fields(job)
    return None


def list_jobs(include_disabled: bool = False) -> List[Dict[str, Any]]:
    jobs = [_apply_skill_fields(j) for j in load_jobs()]
    if not include_disabled:
        jobs = [j for j in jobs if j.get("enabled", True)]
    return jobs


def update_job(job_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    jobs = load_jobs()
    for i, job in enumerate(jobs):
        if job["id"] != job_id:
            continue
        updated = _apply_skill_fields({**job, **updates})
        schedule_changed = "schedule" in updates
        if "skills" in updates or "skill" in updates:
            normalized_skills = _normalize_skill_list(updated.get("skill"), updated.get("skills"))
            updated["skills"] = normalized_skills
            updated["skill"] = normalized_skills[0] if normalized_skills else None
        if schedule_changed:
            updated_schedule = updated["schedule"]
            updated["schedule_display"] = updates.get(
                "schedule_display",
                updated_schedule.get("display", updated.get("schedule_display")),
            )
            if updated.get("state") != "paused":
                updated["next_run_at"] = compute_next_run(updated_schedule)
        # Handle heartbeat_interval_seconds updates
        if "heartbeat_interval_seconds" in updates:
            hb_val = updates["heartbeat_interval_seconds"]
            if hb_val is not None:
                updated["heartbeat_interval_seconds"] = hb_val
                if updated.get("enabled", True) and updated.get("state") != "paused":
                    now = _hermes_now()
                    updated["next_run_at"] = (now + timedelta(seconds=hb_val)).isoformat()
            else:
                updated.pop("heartbeat_interval_seconds", None)
                if updated.get("enabled", True) and updated.get("state") != "paused":
                    sched = updated.get("schedule")
                    if sched:
                        updated["next_run_at"] = compute_next_run(sched)
        # Handle heartbeat config updates (existing handling for 'heartbeat' key)
        if "heartbeat" in updates:
            from cron.heartbeat import configure_job_for_heartbeat
            updated = configure_job_for_heartbeat(updated, updates["heartbeat"])
        if updated.get("enabled", True) and updated.get("state") != "paused" and not updated.get("next_run_at"):
            updated["next_run_at"] = compute_next_run(updated["schedule"])
        jobs[i] = updated
        save_jobs(jobs)
        return _apply_skill_fields(jobs[i])
    return None


def pause_job(job_id: str, reason: Optional[str] = None) -> Optional[Dict[str, Any]]:
    return update_job(
        job_id,
        {
            "enabled": False,
            "state": "paused",
            "paused_at": _hermes_now().isoformat(),
            "paused_reason": reason,
        },
    )


def resume_job(job_id: str) -> Optional[Dict[str, Any]]:
    job = get_job(job_id)
    if not job:
        return None
    next_run_at = compute_next_run(job["schedule"])
    return update_job(
        job_id,
        {
            "enabled": True,
            "state": "scheduled",
            "paused_at": None,
            "paused_reason": None,
            "next_run_at": next_run_at,
        },
    )


def trigger_job(job_id: str) -> Optional[Dict[str, Any]]:
    job = get_job(job_id)
    if not job:
        return None
    return update_job(
        job_id,
        {
            "enabled": True,
            "state": "scheduled",
            "paused_at": None,
            "paused_reason": None,
            "next_run_at": _hermes_now().isoformat(),
        },
    )


def remove_job(job_id: str) -> bool:
    jobs = load_jobs()
    original_len = len(jobs)
    jobs = [j for j in jobs if j["id"] != job_id]
    if len(jobs) < original_len:
        save_jobs(jobs)
        return True
    return False


def mark_job_run(job_id: str, success: bool, error: Optional[str] = None):
    jobs = load_jobs()
    for i, job in enumerate(jobs):
        if job["id"] != job_id:
            continue
        now = _hermes_now()
        now_iso = now.isoformat()
        job["last_run_at"] = now_iso
        job["last_status"] = "ok" if success else "error"
        job["last_error"] = error if not success else None
        if job.get("repeat"):
            job["repeat"]["completed"] = job["repeat"].get("completed", 0) + 1
            times = job["repeat"].get("times")
            completed = job["repeat"]["completed"]
            if times is not None and times > 0 and completed >= times:
                jobs.pop(i)
                save_jobs(jobs)
                return
        # Compute next run, handling heartbeat
        hb_interval = job.get("heartbeat_interval_seconds")
        if hb_interval:
            job["next_run_at"] = (now + timedelta(seconds=hb_interval)).isoformat()
        else:
            job["next_run_at"] = compute_next_run(job["schedule"], now_iso)
        if job["next_run_at"] is None:
            job["enabled"] = False
            job["state"] = "completed"
        elif job.get("state") != "paused":
            job["state"] = "scheduled"
        save_jobs(jobs)
        return
    save_jobs(jobs)


def get_due_jobs() -> List[Dict[str, Any]]:
    now = _hermes_now()
    raw_jobs = load_jobs()
    jobs = [_apply_skill_fields(j) for j in copy.deepcopy(raw_jobs)]
    due = []
    needs_save = False
    for job in jobs:
        if not job.get("enabled", True):
            continue
        next_run = job.get("next_run_at")
        if not next_run:
            recovered_next = _recoverable_oneshot_run_at(
                job.get("schedule", {}),
                now,
                last_run_at=job.get("last_run_at"),
            )
            if not recovered_next:
                continue
            job["next_run_at"] = recovered_next
            next_run = recovered_next
            logger.info(
                "Job '%s' had no next_run_at; recovering one-shot run at %s",
                job.get("name", job["id"]),
                recovered_next,
            )
            for rj in raw_jobs:
                if rj["id"] == job["id"]:
                    rj["next_run_at"] = recovered_next
                    needs_save = True
                    break
        next_run_dt = _ensure_aware(datetime.fromisoformat(next_run))
        if next_run_dt <= now:
            hb_interval = job.get("heartbeat_interval_seconds")
            if hb_interval:
                # Heartbeat: bypass schedule‑based grace, use heartbeat‑specific grace
                grace = max(5, hb_interval // 2)
                if (now - next_run_dt).total_seconds() > grace:
                    new_next = (now + timedelta(seconds=hb_interval)).isoformat()
                    for rj in raw_jobs:
                        if rj["id"] == job["id"]:
                            rj["next_run_at"] = new_next
                            needs_save = True
                            break
                    continue
                due.append(job)
            else:
                # Regular cron/interval handling
                schedule = job.get("schedule", {})
                kind = schedule.get("kind")
                grace = _compute_grace_seconds(schedule)
                if kind in ("cron", "interval") and (now - next_run_dt).total_seconds() > grace:
                    new_next = compute_next_run(schedule, now.isoformat())
                    if new_next:
                        logger.info(
                            "Job '%s' missed its scheduled time (%s, grace=%ds). Fast-forwarding to next run: %s",
                            job.get("name", job["id"]),
                            next_run,
                            grace,
                            new_next,
                        )
                        for rj in raw_jobs:
                            if rj["id"] == job["id"]:
                                rj["next_run_at"] = new_next
                                needs_save = True
                                break
                    continue
                due.append(job)
    if needs_save:
        save_jobs(raw_jobs)
    return due


def save_job_output(job_id: str, output: str):
    ensure_dirs()
    job_output_dir = OUTPUT_DIR / job_id
    job_output_dir.mkdir(parents=True, exist_ok=True)
    _secure_dir(job_output_dir)
    timestamp = _hermes_now().strftime("%Y-%m-%d_%H-%M-%S")
    output_file = job_output_dir / f"{timestamp}.md"
    fd, tmp_path = tempfile.mkstemp(dir=str(job_output_dir), suffix='.tmp', prefix='.output_')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(output)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, output_file)
        _secure_file(output_file)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    return output_file
