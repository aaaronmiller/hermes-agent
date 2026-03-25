"""Heartbeat pattern implementation for Hermes Agent.

Provides:
- job_passes_active_hours
- prepare_heartbeat_execution
- finalize_heartbeat_execution
- configure_job_for_heartbeat
- cmd_heartbeat_active
"""
import os
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

HERMES_DIR = Path(os.getenv("HERMES_HOME", "~/.hermes")).expanduser()

try:
    import pytz
    HAS_PYTZ = True
except ImportError:
    HAS_PYTZ = False


def _now_wrapper():
    """Return current time in Hermes configured timezone."""
    try:
        from hermes_time import now as hermes_now
        return hermes_now()
    except Exception:
        return datetime.now()


def load_heartbeat_config(job: dict) -> dict:
    """Load heartbeat configuration for a job, merging with global defaults."""
    config_path = HERMES_DIR / "config.yaml"
    global_defaults = {}
    try:
        import yaml
        with open(config_path) as f:
            cfg = yaml.safe_load(f) or {}
            global_defaults = cfg.get("heartbeat", {}).get("defaults", {})
    except Exception:
        pass

    job_hb = job.get("heartbeat", {})
    # Merge: job overrides global
    merged = global_defaults.copy()
    merged.update(job_hb)
    return merged


def job_passes_active_hours(job: dict) -> bool:
    """Check if current time is within the job's active hours (if set)."""
    hb_cfg = load_heartbeat_config(job)
    active_hours = hb_cfg.get("active_hours")
    if not active_hours:
        return True

    start = active_hours.get("start")
    end = active_hours.get("end")
    tz_name = active_hours.get("timezone")
    if not start or not end:
        return True

    now = _now_wrapper()
    if tz_name and HAS_PYTZ:
        try:
            import pytz
            tz = pytz.timezone(tz_name)
            now = datetime.now(tz)
        except Exception:
            pass

    # Convert times to comparable objects (assume today's date)
    try:
        start_h, start_m = map(int, start.split(":"))
        end_h, end_m = map(int, end.split(":"))
    except Exception:
        return True

    start_time = now.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
    end_time = now.replace(hour=end_h, minute=end_m, second=0, microsecond=0)
    if end_time <= start_time:
        # Spans midnight
        return now >= start_time or now < end_time
    else:
        return start_time <= now < end_time


def _load_heartbeat_md(job: dict) -> Optional[str]:
    """Load the HEARTBEAT.md for a job if it exists."""
    job_id = job["id"]
    hb_path = HERMES_DIR / "cron" / "workspaces" / job_id / "HEARTBEAT.md"
    if hb_path.exists():
        try:
            return hb_path.read_text(encoding="utf-8")
        except Exception:
            return None
    return None


def prepare_heartbeat_execution(job: dict, prompt: str, skills: list) -> tuple[Optional[str], dict]:
    """Prepare a heartbeat job for execution.

    Returns:
        (new_prompt, execution_context). If new_prompt is None, the job should be skipped.
    """
    hb_cfg = load_heartbeat_config(job)
    if not hb_cfg.get("enabled", False):
        return prompt, {}  # not a heartbeat job, leave prompt unchanged

    # Active hours are already checked by tick or here
    if not job_passes_active_hours(job):
        return None, {}

    execution_context: Dict[str, Any] = {}

    # Inject HEARTBEAT.md content if exists
    hb_md = _load_heartbeat_md(job)
    if hb_md:
        execution_context["heartbeat_md"] = hb_md
        # Prepend or append based on config? Usually prepend to give guidance.
        # We'll prepend a note.
        prompt = f"{hb_md}\n\n---\n\nSystem Instructions:\nYou are running a heartbeat check.\nFollow the checklist above.\n\nUser Prompt:\n{prompt}"

    # Set isolated session flag in context
    execution_context["isolated_session"] = hb_cfg.get("isolated_session", True)
    execution_context["light_context"] = hb_cfg.get("light_context", True)

    return prompt, execution_context


def finalize_heartbeat_execution(job: dict, response: str, execution_context: dict) -> bool:
    """Determine if the response should be suppressed (HEARTBEAT_OK).

    Returns True if suppressed, False otherwise.
    """
    hb_cfg = load_heartbeat_config(job)
    if not hb_cfg.get("enabled", False):
        return False

    stripped = response.strip()
    if stripped.upper() == "HEARTBEAT_OK" and len(stripped) < 300:
        return True

    # Additional heuristics could be added (e.g., check context for askl
    return False


def configure_job_for_heartbeat(job: dict, heartbeat_cfg: dict) -> dict:
    """Merge heartbeat config with global defaults and ensure required fields."""
    # This function is also used by jobs.py; placed here for central logic.
    if "heartbeat" not in job:
        job["heartbeat"] = {}
    job["heartbeat"].update(heartbeat_cfg)
    # Ensure 'enabled' defaults to True if not set
    if "enabled" not in job["heartbeat"]:
        job["heartbeat"]["enabled"] = True
    return job


def cmd_heartbeat_active() -> list:
    """Return recently active heartbeat jobs (for CLI)."""
    from cron.jobs import list_jobs
    jobs = list_jobs(include_disabled=True)
    # Must have heartbeat enabled and have output recently
    recent = [
        j
        for j in jobs
        if j.get("heartbeat", {}).get("enabled", False) and j.get("_last_output_at")
    ]
    # Sort by most recent output
    recent.sort(key=lambda j: j.get("_last_output_at", ""), reverse=True)
    return recent
