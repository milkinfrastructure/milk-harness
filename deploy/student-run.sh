#!/bin/sh
set -eu
umask 077

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
role=$(sed -n '1p' /usr/share/milk/student-artifact-role)
case $role in
  student-train) interpreter=/app/.venv/bin/python ;;
  student-branch) interpreter=/usr/bin/python3 ;;
  *) printf '%s\n' 'student-run: student artifact role authority is invalid' >&2; exit 64 ;;
esac
exec "$interpreter" "$SCRIPT_DIR/student/runtime.py" "$@"
