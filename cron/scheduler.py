"""Cron job scheduler - executes due jobs.

Supports both traditional cron schedules and sub‑minute heartbeats via
heartbeat_interval_seconds. Provides a daemon mode with configurable tick
interval and an 'active' subcommand to list recently running heartbeats.
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import fcntl
except ImportError:
    fcntl = None
    try:
        import msvcrt
    except ImportError:
        msvcrt = None

from hermes_time import now as _hermes_now

logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent.parent))

from cron.jobs import get_due_jobs, mark_job_run, save_job_output, load_jobs
from cron.heartbeat import (
    job_passes_active_hours,
    prepare_heartbeat_execution,
    finalize_heartbeat_execution,
    cmd_heartbeat_active,
)

SILENT_MARKER = "[SILENT]"

_hermes_home = Path(os.getenv("HERMES_HOME", Path.home() / ".hermes"))
_LOCK_DIR = _hermes_home / "cron"
_LOCK_FILE = _LOCK_DIR / ".tick.lock"


def _resolve_origin(job: dict) -> Optional[dict]:
    origin = job.get("origin")
    if not origin:
        return None
    platform = origin.get("platform")
    chat_id = origin.get("chat_id")
    if platform and chat_id:
        return origin
    return None


def _resolve_delivery_target(job: dict) -> Optional[dict]:
    deliver = job.get("deliver", "local")
    origin = _resolve_origin(job)

    if deliver == "local":
        return None
    if deliver == "origin":
        if not origin:
            return None
        return {
            "platform": origin["platform"],
            "chat_id": str(origin["chat_id"]),
            "thread_id": origin.get("thread_id"),
        }
    if ":" in deliver:
        platform_name, rest = deliver.split(":", 1)
        if ":" in rest:
            chat_id, thread_id = rest.split(":", 1)
        else:
            chat_id, thread_id = rest, None
        return {
            "platform": platform_name,
            "chat_id": chat_id,
            "thread_id": thread_id,
        }
    platform_name = deliver
    if origin and origin.get("platform") == platform_name:
        return {
            "platform": platform_name,
            "chat_id": str(origin["chat_id"]),
            "thread_id": origin.get("thread_id"),
        }
    chat_id = os.getenv(f"{platform_name.upper()}_HOME_CHANNEL", "")
    if not chat_id:
        return None
    return {
        "platform": platform_name,
        "chat_id": chat_id,
        "thread_id": None,
    }


def _deliver_result(job: dict, content: str) -> None:
    target = _resolve_delivery_target(job)
    if not target:
        if job.get("deliver", "local") != "local":
            logger.warning(
                "Job '%s' deliver=%s but no concrete delivery target could be resolved",
                job["id"],
                job.get("deliver", "local"),
            )
        return
    platform_name = target["platform"]
    chat_id = target["chat_id"]
    thread_id = target.get("thread_id")
    from tools.send_message_tool import _send_to_platform
    from gateway.config import load_gateway_config, Platform
    platform_map = {
        "telegram": Platform.TELEGRAM,
        "discord": Platform.DISCORD,
        "slack": Platform.SLACK,
        "whatsapp": Platform.WHATSAPP,
        "signal": Platform.SIGNAL,
        "matrix": Platform.MATRIX,
        "mattermost": Platform.MATTERMOST,
        "homeassistant": Platform.HOMEASSISTANT,
        "dingtalk": Platform.DINGTALK,
        "email": Platform.EMAIL,
        "sms": Platform.SMS,
    }
    platform = platform_map.get(platform_name.lower())
    if not platform:
        logger.warning("Job '%s': unknown platform '%s' for delivery", job["id"], platform_name)
        return
    try:
        config = load_gateway_config()
    except Exception as e:
        logger.error("Job '%s': failed to load gateway config for delivery: %s", job["id"], e)
        return
    pconfig = config.platforms.get(platform)
    if not pconfig or not pconfig.enabled:
        logger.warning("Job '%s': platform '%s' not configured/enabled", job["id"], platform_name)
        return
    task_name = job.get("name", job["id"])
    wrapped = (
        f"Cronjob Response: {task_name}\n"
        f"-------------\n\n"
        f"{content}\n\n"
        f"Note: The agent cannot see this message, and therefore cannot respond to it."
    )
    coro = _send_to_platform(platform, pconfig, chat_id, wrapped, thread_id=thread_id)
    try:
        result = asyncio.run(coro)
    except RuntimeError:
        coro.close()
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, _send_to_platform(platform, pconfig, chat_id, wrapped, thread_id=thread_id))
            result = future.result(timeout=30)
    except Exception as e:
        logger.error("Job '%s': delivery to %s:%s failed: %s", job["id"], platform_name, chat_id, e)
        return
    if result and result.get("error"):
        logger.error("Job '%s': delivery error: %s", job["id"], result["error"])
    else:
        logger.info("Job '%s': delivered to %s:%s", job["id"], platform_name, chat_id)


def _build_job_prompt(job: dict) -> str:
    prompt = job.get("prompt", "")
    skills = job.get("skills")
    silent_hint = (
        "[SYSTEM: If you have nothing new or noteworthy to report, respond "
        "with exactly \"[SILENT]\" (optionally followed by a brief internal "
        "note). This suppresses delivery to the user while still saving "
        "output locally. Only use [SILENT] when there are genuinely no "
        "changes worth reporting.]\n\n"
    )
    prompt = silent_hint + prompt
    if skills is None:
        legacy = job.get("skill")
        skills = [legacy] if legacy else []
    skill_names = [str(name).strip() for name in skills if str(name).strip()]
    if not skill_names:
        return prompt
    from tools.skills_tool import skill_view
    parts = []
    skipped: list[str] = []
    for skill_name in skill_names:
        loaded = json.loads(skill_view(skill_name))
        if not loaded.get("success"):
            error = loaded.get("error") or f"Failed to load skill '{skill_name}'"
            logger.warning(
                "Cron job '%s': skill not found, skipping — %s",
                job.get("name", job.get("id")),
                error,
            )
            skipped.append(skill_name)
            continue
        content = str(loaded.get("content") or "").strip()
        if parts:
            parts.append("")
        parts.extend(
            [
                f'[SYSTEM: The user has invoked the "{skill_name}" skill, indicating they want you to follow its instructions. The full skill content is loaded below.]',
                "",
                content,
            ]
        )
    if skipped:
        notice = (
            f"[SYSTEM: The following skill(s) were listed for this job but could not be found "
            f"and were skipped: {', '.join(skipped)}. "
            f"Start your response with a brief notice so the user is aware, e.g.: "
            f"'⚠️ Skill(s) not found and skipped: {', '.join(skipped)}']"
        )
        parts.insert(0, notice)
    if prompt:
        parts.extend(["", f"The user has provided the following instruction alongside the skill invocation: {prompt}"])
    return "\n".join(parts)


def run_job(job: dict) -> tuple[bool, str, str, Optional[str]]:
    from run_agent import AIAgent
    _session_db = None
    try:
        from hermes_state import SessionDB
        _session_db = SessionDB()
    except Exception as e:
        logger.debug("Job '%s': SQLite session store not available: %s", job.get("id", "?"), e)
    job_id = job["id"]
    job_name = job["name"]
    prompt = _build_job_prompt(job)
    origin = _resolve_origin(job)
    logger.info("Running job '%s' (ID: %s)", job_name, job_id)
    logger.info("Prompt: %s", prompt[:100])
    execution_context = {}
    if job.get("heartbeat", {}).get("enabled", False):
        prompt, execution_context = prepare_heartbeat_execution(job, prompt, job.get("skills", []))
        if prompt is None:
            output = "(Heartbeat job skipped due to active hours or configuration)"
            return True, output, output, None
    try:
        from dotenv import load_dotenv
        try:
            load_dotenv(str(_hermes_home / ".env"), override=True, encoding="utf-8")
        except UnicodeDecodeError:
            load_dotenv(str(_hermes_home / ".env"), override=True, encoding="latin-1")
        delivery_target = _resolve_delivery_target(job)
        if delivery_target:
            os.environ["HERMES_CRON_AUTO_DELIVER_PLATFORM"] = delivery_target["platform"]
            os.environ["HERMES_CRON_AUTO_DELIVER_CHAT_ID"] = str(delivery_target["chat_id"])
            if delivery_target.get("thread_id") is not None:
                os.environ["HERMES_CRON_AUTO_DELIVER_THREAD_ID"] = str(delivery_target["thread_id"])
        model = job.get("model") or os.getenv("HERMES_MODEL") or "anthropic/claude-opus-4.6"
        _cfg = {}
        try:
            import yaml
            _cfg_path = str(_hermes_home / "config.yaml")
            if os.path.exists(_cfg_path):
                with open(_cfg_path) as _f:
                    _cfg = yaml.safe_load(_f) or {}
                _model_cfg = _cfg.get("model", {})
                if not job.get("model"):
                    if isinstance(_model_cfg, str):
                        model = _model_cfg
                    elif isinstance(_model_cfg, dict):
                        model = _model_cfg.get("default", model)
        except Exception as e:
            logger.warning("Job '%s': failed to load config.yaml, using defaults: %s", job_id, e)
        reasoning_config = None
        effort = os.getenv("HERMES_REASONING_EFFORT", "") or str(_cfg.get("agent", {}).get("reasoning_effort", "")).strip()
        if effort and effort.lower() != "none":
            valid = ("xhigh", "high", "medium", "low", "minimal")
            if effort.lower() in valid:
                reasoning_config = {"enabled": True, "effort": effort.lower()}
        elif effort.lower() == "none":
            reasoning_config = {"enabled": False}
        prefill_messages = None
        prefill_file = os.getenv("HERMES_PREFILL_MESSAGES_FILE", "") or _cfg.get("prefill_messages_file", "")
        if prefill_file:
            import json as _json
            pfpath = Path(prefill_file).expanduser()
            if not pfpath.is_absolute():
                pfpath = _hermes_home / pfpath
            if pfpath.exists():
                try:
                    with open(pfpath, "r", encoding="utf-8") as _pf:
                        prefill_messages = _json.load(_pf)
                    if not isinstance(prefill_messages, list):
                        prefill_messages = None
                except Exception as e:
                    logger.warning("Job '%s': failed to parse prefill messages file '%s': %s", job_id, pfpath, e)
                    prefill_messages = None
        max_iterations = _cfg.get("agent", {}).get("max_turns") or _cfg.get("max_turns") or 90
        pr = _cfg.get("provider_routing", {})
        smart_routing = _cfg.get("smart_model_routing", {}) or {}
        from hermes_cli.runtime_provider import resolve_runtime_provider, format_runtime_provider_error
        try:
            runtime_kwargs = {
                "requested": job.get("provider") or os.getenv("HERMES_INFERENCE_PROVIDER"),
            }
            if job.get("base_url"):
                runtime_kwargs["explicit_base_url"] = job["base_url"]
            runtime = resolve_runtime_provider(**runtime_kwargs)
        except Exception as exc:
            message = format_runtime_provider_error(exc)
            raise RuntimeError(message) from exc
        from agent.smart_model_routing import resolve_turn_route
        turn_route = resolve_turn_route(
            prompt,
            smart_routing,
            {
                "model": model,
                "api_key": runtime.get("api_key"),
                "base_url": runtime.get("base_url"),
                "provider": runtime.get("provider"),
                "api_mode": runtime.get("api_mode"),
                "command": runtime.get("command"),
                "args": list(runtime.get("args") or []),
            },
        )
        agent = AIAgent(
            model=turn_route["model"],
            api_key=turn_route["runtime"].get("api_key"),
            base_url=turn_route["runtime"].get("base_url"),
            provider=turn_route["runtime"].get("provider"),
            api_mode=turn_route["runtime"].get("api_mode"),
            acp_command=turn_route["runtime"].get("command"),
            acp_args=turn_route["runtime"].get("args"),
            max_iterations=max_iterations,
            reasoning_config=reasoning_config,
            prefill_messages=prefill_messages,
            providers_allowed=pr.get("only"),
            providers_ignored=pr.get("ignore"),
            providers_order=pr.get("order"),
            provider_sort=pr.get("sort"),
            disabled_toolsets=["cronjob", "messaging", "clarify"],
            quiet_mode=True,
            platform="cron",
            session_id=f"cron_{job_id}_{_hermes_now().strftime('%Y%m%d_%H%M%S')}",
            session_db=_session_db,
        )
        result = agent.run_conversation(prompt)
        final_response = result.get("final_response", "") or ""
        suppressed = False
        if job.get("heartbeat", {}).get("enabled", False):
            suppressed = finalize_heartbeat_execution(job, final_response, execution_context)
            if suppressed:
                job["_suppress_delivery"] = True
        logged_response = final_response if final_response else "(No response generated)"
        output = f"""# Cron Job: {job_name}

**Job ID:** {job_id}
**Run Time:** {_hermes_now().strftime('%Y-%m-%d %H:%M:%S')}
**Schedule:** {job.get('schedule_display', 'N/A')}

## Prompt

{prompt}

## Response

{logged_response}
"""
        logger.info("Job '%s' completed successfully", job_name)
        return True, output, final_response, None
    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        logger.error("Job '%s' failed: %s", job_name, error_msg)
        output = f"""# Cron Job: {job_name} (FAILED)

**Job ID:** {job_id}
**Run Time:** {_hermes_now().strftime('%Y-%m-%d %H:%M:%S')}
**Schedule:** {job.get('schedule_display', 'N/A')}

## Prompt

{prompt}

## Error

```
{error_msg}

{traceback.format_exc()}
```
"""
        return False, output, "", error_msg
    finally:
        for key in (
            "HERMES_SESSION_PLATFORM",
            "HERMES_SESSION_CHAT_ID",
            "HERMES_SESSION_CHAT_NAME",
            "HERMES_CRON_AUTO_DELIVER_PLATFORM",
            "HERMES_CRON_AUTO_DELIVER_CHAT_ID",
            "HERMES_CRON_AUTO_DELIVER_THREAD_ID",
        ):
            os.environ.pop(key, None)
        if _session_db:
            try:
                _session_db.close()
            except Exception as e:
                logger.debug("Job '%s': failed to close SQLite session store: %s", job_id, e)


def tick(verbose: bool = True) -> int:
    _LOCK_DIR.mkdir(parents=True, exist_ok=True)
    lock_fd = None
    try:
        lock_fd = open(_LOCK_FILE, "w")
        if fcntl:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        elif msvcrt:
            msvcrt.locking(lock_fd.fileno(), msvcrt.LK_NBLCK, 1)
    except (OSError, IOError):
        logger.debug("Tick skipped — another instance holds the lock")
        if lock_fd is not None:
            lock_fd.close()
        return 0
    try:
        now = _hermes_now()
        due_jobs = get_due_jobs()
        # Filter heartbeats outside active hours
        filtered = []
        for job in due_jobs:
            if job.get("heartbeat", {}).get("enabled", False) and job.get("heartbeat_interval_seconds"):
                if not job_passes_active_hours(job):
                    continue
            filtered.append(job)
        due_jobs = filtered
        if verbose and not due_jobs:
            logger.info("%s - No jobs due", now.strftime('%H:%M:%S'))
            return 0
        if verbose:
            logger.info("%s - %s job(s) due", now.strftime('%H:%M:%S'), len(due_jobs))
        executed = 0
        for job in due_jobs:
            try:
                success, output, final_response, error = run_job(job)
                output_file = save_job_output(job["id"], output)
                if verbose:
                    logger.info("Output saved to: %s", output_file)
                deliver_content = final_response if success else f"⚠️ Cron job '{job.get('name', job['id'])}' failed:\n{error}"
                should_deliver = bool(deliver_content)
                if should_deliver and success and deliver_content.strip().upper().startswith(SILENT_MARKER):
                    logger.info("Job '%s': agent returned %s — skipping delivery", job["id"], SILENT_MARKER)
                    should_deliver = False
                if should_deliver and job.get("_suppress_delivery", False):
                    logger.info("Job '%s': heartbeat suppression — skipping delivery", job["id"])
                    should_deliver = False
                if should_deliver:
                    try:
                        _deliver_result(job, deliver_content)
                    except Exception as de:
                        logger.error("Delivery failed for job %s: %s", job["id"], de)
                is_heartbeat = bool(job.get("heartbeat_interval_seconds"))
                if is_heartbeat:
                    now_ts = _hermes_now().isoformat()
                    job["last_run_at"] = now_ts
                    job["last_status"] = "ok" if success else "error"
                    job["last_error"] = error if not success else None
                    hb_interval = job["heartbeat_interval_seconds"]
                    job["next_run_at"] = (_hermes_now() + timedelta(seconds=hb_interval)).isoformat()
                    try:
                        all_jobs = load_jobs()
                        for i, j in enumerate(all_jobs):
                            if j["id"] == job["id"]:
                                all_jobs[i] = job
                                save_jobs(all_jobs)
                                break
                    except Exception as e:
                        logger.warning("Failed to persist heartbeat state: %s", e)
                else:
                    mark_job_run(job["id"], success, error)
                executed += 1
            except Exception as e:
                logger.error("Error processing job %s: %s", job['id'], e)
                try:
                    if not job.get("heartbeat_interval_seconds"):
                        mark_job_run(job["id"], False, str(e))
                except Exception:
                    pass
        return executed
    finally:
        try:
            if fcntl:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            elif msvcrt:
                try:
                    msvcrt.locking(lock_fd.fileno(), msvcrt.LK_UNLCK, 1)
                except (OSError, IOError):
                    pass
        except Exception:
            pass
        if lock_fd:
            lock_fd.close()


def load_config() -> dict:
    config_path = _hermes_home / "config.yaml"
    try:
        import yaml
        with open(config_path) as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}

def get_scheduler_config() -> dict:
    return load_config().get("scheduler", {})


def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if argv and argv[0] == "active":
        import json
        active = cmd_heartbeat_active()
        print(json.dumps({"active_heartbeats": active}, indent=2, ensure_ascii=False))
        return 0
    parser = argparse.ArgumentParser(description="Hermes cron daemon")
    parser.add_argument("--once", action="store_true", help="Run due jobs once and exit")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if args.once:
        return tick(verbose=args.verbose)
    else:
        tick_interval = get_scheduler_config().get("tick_interval_seconds", 1)
        logger.info("cron daemon started (tick interval: %ds)", tick_interval)
        try:
            while True:
                tick(verbose=args.verbose)
                time.sleep(tick_interval)
        except KeyboardInterrupt:
            logger.info("cron daemon stopped")
            return 0

if __name__ == "__main__":
    sys.exit(main())
