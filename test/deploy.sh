#!/bin/bash
# Protocolo de deploy — PARA no primeiro vermelho. set -e + sem pipe no teste.
set -e
cd /tmp/gmb/repo
echo "── harness ──"
node test/harness.js > /tmp/h.log 2>&1 || { tail -6 /tmp/h.log; echo "🔴 HARNESS VERMELHO — deploy abortado"; exit 1; }
tail -2 /tmp/h.log
echo "── auditoria ──"
node test/auditoria.js > /tmp/a.log 2>&1 || { tail -8 /tmp/a.log; echo "🔴 AUDITORIA COM PROBLEMA NOVO — deploy abortado"; exit 1; }
tail -2 /tmp/a.log
echo "── publicando ──"
git add -A && git commit -q -m "$1" && git push origin dev -q
git checkout main -q && git merge dev --no-edit -q && git push origin main -q
echo "MERGE OK"; sleep 66; echo "PUBLICADO"
