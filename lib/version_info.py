# lib/version_info.py
# Serves the deployed commit's info at /version (adapted from upstream 9c88a0d,
# which used WSGI internals our Flask app doesn't have).
import subprocess
from pathlib import Path

from lib import logger


def get_git_info() -> tuple[str, str, str]:
    """Return (commit_hash, commit_date, commit_subject) or graceful errors."""
    repo_dir = Path(__file__).resolve().parent.parent
    try:
        result = subprocess.run(
            ['git', 'log', '-1', '--format=%H%n%ci%n%s'],
            cwd=repo_dir,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        lines = result.stdout.strip().split('\n') if result.stdout else []
        commit_hash = lines[0].strip() if len(lines) >= 1 else 'N/A'
        commit_date = lines[1].strip() if len(lines) >= 2 else 'N/A'
        commit_subject = lines[2].strip() if len(lines) >= 3 else 'N/A'
        return commit_hash, commit_date, commit_subject
    except Exception as e:  # git missing / not a repo / timeout on serverless
        logger.error(f'Git info retrieval failed: {e}')
        return (
            'ERROR',
            type(e).__name__,
            'Check if git is installed and if this is a repository.',
        )
