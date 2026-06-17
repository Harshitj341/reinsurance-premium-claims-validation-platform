"""
Platform Module: Core Management User Interface
Domain: Reinsurance Lifecycle Data Control Plane

Description:
    Provides interactive management dashboards for client data schemas,
    Statement of Account (SOA) matching balances, and engine validation runs.
"""

import os
import json
import collections
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import Json, DictCursor, RealDictCursor
from flask import Flask, render_template_string, request, redirect, url_for

app = Flask(__name__)

# ==============================================================================
# DATABASE LAYER ARCHITECTURE
# ==============================================================================
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "airflow")
DB_USER = os.getenv("POSTGRES_USER", "airflow")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "airflow")

def get_db():
    """Establishes an isolated transactional connection instance to PostgreSQL."""
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

# ==============================================================================
# FRONTEND ENGINE LAYOUT DEFINITIONS (CSS & BASE STYLES)
# ==============================================================================
BASE_STYLE = """
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap" rel="stylesheet">
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --bg:        #0d0f12;
    --surface:   #141720;
    --border:    #252a35;
    --text:      #e2e8f0;
    --muted:     #64748b;
    --accent:    #38bdf8;
    --warn:      #f59e0b;
    --danger:    #ef4444;
    --success:   #22c55e;
    --mono:      'IBM Plex Mono', monospace;
    --sans:      'IBM Plex Sans', sans-serif;
  }
  body { background: var(--bg); color: var(--text); font-family: var(--sans); font-size: 14px; min-height: 100vh; padding: 0; }
  header { border-bottom: 1px solid var(--border); padding: 18px 40px; display: flex; align-items: center; gap: 12px; background: var(--surface); }
  header .logo { font-family: var(--mono); font-size: 13px; font-weight: 600; color: var(--accent); letter-spacing: 0.08em; text-transform: uppercase; }
  header .divider { color: var(--border); margin: 0 4px; }
  header .title { font-size: 13px; color: var(--muted); font-weight: 300; }
  main { padding: 36px 40px; max-width: 1100px; }
  h2 { font-family: var(--mono); font-size: 11px; font-weight: 600; letter-spacing: 0.12em; text-transform: uppercase; color: var(--muted); margin-bottom: 16px; padding-bottom: 8px; border-bottom: 1px solid var(--border); }
  .section { margin-bottom: 48px; }
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 6px; padding: 20px 24px; margin-bottom: 12px; display: grid; grid-template-columns: 1fr auto; align-items: center; gap: 16px; }
  .card:hover { border-color: #333a48; }
  .card-meta { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin-bottom: 8px; }
  .tag { font-family: var(--mono); font-size: 11px; padding: 2px 8px; border-radius: 3px; font-weight: 600; letter-spacing: 0.05em; }
  .tag-client  { background: #1e3a5f; color: var(--accent); }
  .tag-cat     { background: #1e2a1e; color: var(--success); }
  .tag-pending { background: #2a1e00; color: var(--warn); }
  .tag-affects { background: #2a1a00; color: #fb923c; }
  .card-col { font-family: var(--mono); font-size: 15px; font-weight: 600; color: var(--text); margin-bottom: 4px; }
  .card-samples { font-family: var(--mono); font-size: 11px; color: var(--muted); }
  .card-created { font-size: 11px; color: var(--muted); margin-top: 4px; }
  .btn { font-family: var(--mono); font-size: 11px; font-weight: 600; letter-spacing: 0.08em; text-transform: uppercase; padding: 8px 18px; border-radius: 4px; border: none; cursor: pointer; text-decoration: none; display: inline-block; transition: opacity 0.15s; }
  .btn:hover { opacity: 0.85; }
  .btn-primary  { background: var(--accent); color: #0d0f12; }
  .btn-success  { background: var(--success); color: #0d0f12; }
  .btn-warn     { background: var(--warn); color: #0d0f12; }
  .btn-danger   { background: var(--danger); color: #fff; }
  .btn-ghost    { background: transparent; border: 1px solid var(--border); color: var(--muted); }
  .form-card { background: var(--surface); border: 1px solid var(--border); border-radius: 6px; padding: 32px; max-width: 680px; }
  .field { margin-bottom: 24px; }
  .field label { display: block; font-family: var(--mono); font-size: 11px; font-weight: 600; letter-spacing: 0.1em; text-transform: uppercase; color: var(--muted); margin-bottom: 8px; }
  .field input[type="text"], .field input[type="number"], .field textarea { width: 100%; background: var(--bg); border: 1px solid var(--border); border-radius: 4px; padding: 10px 14px; color: var(--text); font-family: var(--mono); font-size: 13px; outline: none; transition: border-color 0.15s; }
  .field input:focus, .field textarea:focus { border-color: var(--accent); }
  .field input[readonly] { color: var(--muted); cursor: not-allowed; }
  .checkbox-row { display: flex; gap: 32px; margin-bottom: 24px; }
  .checkbox-row label { display: flex; align-items: center; gap: 8px; font-size: 13px; color: var(--text); cursor: pointer; }
  .checkbox-row input[type="checkbox"] { width: 16px; height: 16px; accent-color: var(--accent); cursor: pointer; }
  .action-row { display: flex; gap: 12px; margin-top: 32px; flex-wrap: wrap; }
  .info-block { background: var(--bg); border: 1px solid var(--border); border-radius: 4px; padding: 12px 16px; font-family: var(--mono); font-size: 12px; color: var(--muted); margin-bottom: 24px; line-height: 1.7; }
  .info-block span { color: var(--text); }
  .warn-box { background: #1a1200; border: 1px solid #92400e; border-radius: 4px; padding: 12px 16px; font-size: 12px; color: #fcd34d; margin-bottom: 24px; line-height: 1.6; }
  .empty { font-size: 13px; color: var(--muted); font-family: var(--mono); padding: 20px 0; }
  .resolve-form { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
  .resolve-form input[type="text"] { background: var(--bg); border: 1px solid var(--border); border-radius: 4px; padding: 6px 12px; color: var(--text); font-family: var(--mono); font-size: 12px; outline: none; width: 180px; }
  .resolve-form input[type="text"]:focus { border-color: var(--success); }
  .separator { height: 1px; background: var(--border); margin: 8px 0 20px; }
</style>
"""

INDEX_HTML = """<!DOCTYPE html><html><head>{{ base_style | safe }}<title>Mapping Review</title></head><body><header><span class="logo">RI Platform</span><span class="divider">/</span><span class="title">Schema Mapping Review</span><a href="/soa" style="color:var(--muted);text-decoration:none;font-size:13px;margin-left:24px">SOA Entry</a><a href="/validation" style="color:var(--muted);text-decoration:none;font-size:13px;margin-left:24px">Validation</a></header><main><div class="section"><h2>Pending Review &nbsp;({{ pending|length }})</h2>{% if pending %}{% for row in pending %}<div class="card"><div><div class="card-meta"><span class="tag tag-client">{{ row[1] }}</span><span class="tag tag-cat">{{ row[2] }}</span><span class="tag tag-pending">PENDING</span></div><div class="card-col">{{ row[3] }}</div><div class="card-samples">samples: {{ row[4] or '—' }}</div><div class="card-created">received {{ row[6] }}</div></div><a href="/review/{{ row[0] }}" class="btn btn-primary">Review</a></div>{% endfor %}{% else %}<p class="empty">No pending items.</p>{% endif %}</div><div class="section"><h2>Awaiting DE Resolution &nbsp;({{ needs_de|length }})</h2>{% if needs_de %}{% for row in needs_de %}<div class="card"><div><div class="card-meta"><span class="tag tag-client">{{ row[1] }}</span><span class="tag tag-cat">{{ row[2] }}</span><span class="tag tag-affects">AFFECTS CALCULATIONS</span></div><div class="card-col">{{ row[3] }} → <span style="color:var(--accent)">{{ row[4] or '?' }}</span></div><div class="card-created">reviewed by {{ row[8] or '—' }} · pipeline blocked until DE resolves</div></div><form class="resolve-form" method="POST" action="/resolve/{{ row[0] }}"><input type="text" name="de_name" placeholder="Your name" required><button type="submit" class="btn btn-success">Mark Resolved</button></form></div>{% endfor %}{% else %}<p class="empty">Nothing waiting on DE.</p>{% endif %}</div></main></body></html>"""
REVIEW_HTML = """<!DOCTYPE html><html><head>{{ base_style | safe }}<title>Review Column</title></head><body><header><span class="logo">RI Platform</span><span class="divider">/</span><a href="/" style="color:var(--muted);text-decoration:none;font-size:13px">Queue</a><span class="divider">/</span><span class="title">Review Column</span></header><main><div class="form-card"><div class="info-block">Client &nbsp;&nbsp;&nbsp;<span>{{ item[1] }}</span><br>Category &nbsp;<span>{{ item[2] }}</span><br>Raw col &nbsp;&nbsp;<span>{{ item[3] }}</span><br>Samples &nbsp;&nbsp;<span>{{ item[4] or '—' }}</span></div><div class="warn-box">⚠ &nbsp;If this column affects premium or claims calculations, select <strong>Affects Calculations</strong>. The pipeline will stay blocked until a DE confirms the silver logic has been updated.</div><form method="POST" action="/review/{{ item[0] }}"><div class="field"><label>Canonical Name (our internal name)</label><input type="text" name="canonical_name" placeholder="e.g. premium" required></div><div class="field"><label>Reviewed By</label><input type="text" name="reviewed_by" placeholder="Your name" required></div><div class="checkbox-row"><label><input type="checkbox" name="is_critical">Critical column</label><label><input type="checkbox" name="is_pii">Contains PII</label></div><div class="separator"></div><div class="action-row"><button type="submit" name="action" value="approve" class="btn btn-success">✓ &nbsp;Approve</button><button type="submit" name="action" value="affects_calculations" class="btn btn-warn">⚡ &nbsp;Affects Calculations</button><button type="submit" name="action" value="reject" class="btn btn-danger">✕ &nbsp;Reject</button><a href="/" class="btn btn-ghost">Cancel</a></div></form></div></main></body></html>"""
SOA_DASHBOARD_HTML = """<!DOCTYPE html><html><head>{{ base_style | safe }}<title>SOA Entry</title></head><body><header><span class="logo">RI Platform</span><span class="divider">/</span><span class="title">SOA Entry</span><a href="/" style="color:var(--muted);text-decoration:none;font-size:13px;margin-left:24px">Mapping Review</a><a href="/validation" style="color:var(--muted);text-decoration:none;font-size:13px;margin-left:24px">Validation</a></header><main><div class="section"><h2>Action Required &nbsp;({{ action_required|length }})</h2>{% if action_required %}{% for row in action_required %}<div class="card"><div><div class="card-meta"><span class="tag tag-client">{{ row.client_id }}</span><span class="tag tag-pending">{{ row.status }}</span></div><div class="card-col">{{ row.year }} / {{ row.period }}</div>{% if row.version %}<div class="card-samples">Version: {{ row.version }}</div>{% endif %}</div><a class="btn btn-primary" href="/soa/enter?client_id={{ row.client_id }}&year={{ row.year }}&period={{ row.period }}">{% if row.status == 'REJECTED' %}Edit{% else %}Enter SOA{% endif %}</a></div>{% endfor %}{% else %}<p class="empty">No actions required.</p>{% endif %}</div><div class="section"><h2>Pending Approval &nbsp;({{ pending_approval|length }})</h2>{% if pending_approval %}{% for row in pending_approval %}<div class="card"><div><div class="card-meta"><span class="tag tag-client">{{ row.client_id }}</span><span class="tag tag-pending">PENDING_APPROVAL</span></div><div class="card-col">{{ row.year }} / {{ row.period }} (v{{ row.version }})</div><div class="card-samples">Net SOA: {{ row.net_soa }}</div><div class="card-created">entered by {{ row.entered_by }}</div></div><a class="btn btn-primary" href="/soa/approve/{{ row.id }}">Review</a></div>{% endfor %}{% else %}<p class="empty">No entries awaiting approval.</p>{% endif %}</div><div class="section"><h2>Approved &nbsp;({{ approved|length }})</h2>{% if approved %}{% for row in approved %}<div class="card"><div><div class="card-meta"><span class="tag tag-client">{{ row.client_id }}</span><span class="tag tag-cat">APPROVED</span></div><div class="card-col">{{ row.year }} / {{ row.period }} (v{{ row.version }})</div><div class="card-samples">Net SOA: {{ row.net_soa }}</div><div class="card-created">approved by {{ row.approved_by }}</div></div></div>{% endfor %}{% else %}<p class="empty">No approved entries.</p>{% endif %}</div><div class="section"><h2>Reconciled &nbsp;({{ reconciled|length }})</h2>{% if reconciled %}{% for row in reconciled %}<div class="card"><div><div class="card-meta"><span class="tag tag-client">{{ row.client_id }}</span><span class="tag tag-cat" style="background:#1e3a5f;">RECONCILED</span></div><div class="card-col">{{ row.year }} / {{ row.period }} (v{{ row.version }})</div><div class="card-samples">Net SOA: {{ row.net_soa }}</div></div></div>{% endfor %}{% else %}<p class="empty">No reconciled entries.</p>{% endif %}</div><div class="section"><h2>Adjustment Candidates &nbsp;({{ adjustment_candidates|length }})</h2>{% if adjustment_candidates %}{% for row in adjustment_candidates %}<div class="card"><div><div class="card-meta"><span class="tag tag-client">{{ row.client_id }}</span><span class="tag tag-warn">ALREADY RECONCILED</span></div><div class="card-col">{{ row.year }} / {{ row.period }}</div><div class="card-samples">Latest Active Version: {{ row.version }}</div></div><div style="display:flex;gap:8px;"><a class="btn btn-warn" href="/soa/enter?client_id={{ row.client_id }}&year={{ row.year }}&period={{ row.period }}&period_type={{ row.period_type }}&submission_type=ADJUSTMENT">Create Adjustment</a><form method="POST" action="/soa/duplicate/{{ row.id }}" style="display:inline;"><button type="submit" class="btn btn-ghost">Mark Duplicate</button></form></div></div>{% endfor %}{% else %}<p class="empty">No adjustment candidates.</p>{% endif %}</div></main></body></html>"""
SOA_ENTRY_HTML = """<!DOCTYPE html><html><head>{{ base_style | safe }}<title>Enter SOA Figures</title></head><body><header><span class="logo">RI Platform</span><span class="divider">/</span><a href="/soa" style="color:var(--muted);text-decoration:none;font-size:13px">SOA Entry</a><span class="divider">/</span><span class="title">Enter Figures</span></header><main><div class="form-card">{% if error %}<div class="warn-box">{{ error }}</div>{% endif %}<div class="info-block">Client &nbsp;&nbsp;<span>{{ client_id }}</span><br>Period &nbsp;&nbsp;<span>{{ year }} / {{ period }} ({{ period_type }})</span></div><form method="POST" id="soa-entry-form"><input type="hidden" name="client_id" value="{{ client_id }}"><input type="hidden" name="year" value="{{ year }}"><input type="hidden" name="period" value="{{ period }}"><input type="hidden" name="period_type" value="{{ period_type }}"><input type="hidden" name="additional_items_json" id="additional-items-json" value="[]"><div class="field"><label>Premium SOA</label><input type="number" name="premium_soa" id="premium-soa" step="0.01" required></div><div class="field"><label>Claims SOA</label><input type="number" name="claims_soa" id="claims-soa" step="0.01" required></div><div class="field"><label>Commission SOA</label><input type="number" name="commission_soa" id="commission-soa" step="0.01" required></div><div class="field"><label>Tax SOA</label><input type="number" name="tax_soa" id="tax-soa" step="0.01" required></div><div class="field"><label>Entered By</label><input type="text" name="entered_by" required></div><h2>Additional Items</h2><div id="additional-items"></div><button type="button" class="btn btn-ghost" onclick="addItem()" style="margin-top:8px;">Add Item</button><div class="info-block" style="margin-top:24px">Net SOA &nbsp;<span id="net-preview">0.00</span></div><div class="action-row"><button type="submit" class="btn btn-success">Submit for Approval</button><a class="btn btn-ghost" href="/soa">Cancel</a></div></form></div></main><script>const items = document.getElementById("additional-items"); const numberValue = (value) => Number.parseFloat(value || "0") || 0; function addItem(){const row = document.createElement("div"); row.className = "action-row soa-item"; row.style.marginTop = "12px"; row.innerHTML = `<input type="text" class="item-label" placeholder="e.g. Recapture Fee" style="flex:1;min-width:180px;background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:8px;color:var(--text)"> <input type="number" class="item-amount" step="0.01" placeholder="Amount" style="width:140px;background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:8px;color:var(--text)"> <select class="item-direction" style="background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:8px;color:var(--text)"> <option value="debit">Debit</option> <option value="credit">Credit</option> </select> <button type="button" class="btn btn-danger" onclick="this.parentElement.remove(); updateNet()">Remove</button>`; row.addEventListener("input", updateNet); items.appendChild(row);}function collectItems(){return [...document.querySelectorAll(".soa-item")].map((row) => ({label: row.querySelector(".item-label").value.trim(), amount: numberValue(row.querySelector(".item-amount").value), direction: row.querySelector(".item-direction").value})).filter((item) => item.label || item.amount);}function updateNet(){let net = numberValue(document.getElementById("premium-soa").value) - numberValue(document.getElementById("claims-soa").value) - numberValue(document.getElementById("commission-soa").value) - numberValue(document.getElementById("tax-soa").value); collectItems().forEach((item) => {net += item.direction === "credit" ? item.amount : -item.amount;}); document.getElementById("net-preview").textContent = net.toFixed(2);}document.getElementById("soa-entry-form").addEventListener("submit", () => {document.getElementById("additional-items-json").value = JSON.stringify(collectItems());}); ["premium-soa", "claims-soa", "commission-soa", "tax-soa"].forEach(id => {document.getElementById(id).addEventListener("input", updateNet);});</script></body></html>"""
SOA_APPROVE_HTML = """<!DOCTYPE html><html><head>{{ base_style | safe }}<title>Approve SOA</title></head><body><header><span class="logo">RI Platform</span><span class="divider">/</span><a href="/soa" style="color:var(--muted);text-decoration:none;font-size:13px">SOA Entry</a><span class="divider">/</span><span class="title">Approve</span></header><main><div class="form-card">{% if error %}<div class="warn-box">{{ error }}</div>{% endif %}<div class="info-block">Client &nbsp;&nbsp;<span>{{ entry[1] }}</span><br>Period &nbsp;&nbsp;<span>{{ entry[2] }} / {{ entry[3] }}</span><br>Version &nbsp;<span>v{{ entry[12] }}</span><br>Entered &nbsp;<span>{{ entry[11] }} at {{ entry[13] }}</span></div><div class="info-block">Premium SOA &nbsp;&nbsp;&nbsp;&nbsp;<span>{{ entry[5] }}</span><br>Claims SOA &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span>{{ entry[6] }}</span><br>Commission SOA &nbsp;<span>{{ entry[7] }}</span><br>Tax SOA &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span>{{ entry[8] }}</span><br>Net SOA &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span>{{ entry[9] }}</span></div>{% if additional_items %}<h2>Additional Items</h2><div class="info-block">{% for item in additional_items %}<span>{{ item.label }}</span> &nbsp; {{ item.direction }} &nbsp; {{ item.amount }}<br>{% endfor %}</div>{% endif %}<div class="warn-box">You cannot be the same person who entered these figures. This is enforced at database level.</div><form method="POST"><div class="field"><label>Approved By</label><input type="text" name="approved_by" required></div><div class="field" id="rejection-field" style="display:none"><label>Rejection Reason</label><textarea name="rejection_reason" id="rejection-reason" rows="4"></textarea></div><div class="action-row"><button type="submit" name="action" value="approve" class="btn btn-success">Approve</button><button type="submit" name="action" value="reject" class="btn btn-danger" onclick="return prepareReject(event)">Reject</button><a class="btn btn-ghost" href="/soa">Cancel</a></div></form></div></main><script>function prepareReject(event){const field = document.getElementById("rejection-field"); const reason = document.getElementById("rejection-reason"); if (field.style.display === "none"){event.preventDefault(); field.style.display = "block"; reason.required = true; reason.focus(); return false;}return true;}</script></body></html>"""
SOA_ADJUSTMENT_HTML = """<!DOCTYPE html><html><head>{{ base_style | safe }}<title>Adjust SOA</title></head><body><header><span class="logo">RI Platform</span><span class="divider">/</span><a href="/soa" style="color:var(--muted);text-decoration:none;font-size:13px">SOA Entry</a><span class="divider">/</span><span class="title">Adjustment</span></header><main><div class="form-card"><div class="warn-box">Existing Reconciled Period Detected. You are about to initiate an adjustment for a closed period.</div><div class="info-block">Client &nbsp;&nbsp;<span>{{ entry[1] }}</span><br>Period &nbsp;&nbsp;<span>{{ entry[2] }} / {{ entry[3] }}</span><br>Current Version &nbsp;<span>v{{ entry[12] }}</span><br></div><div class="action-row"><a class="btn btn-warn" href="/soa/enter?client_id={{ entry[1] }}&year={{ entry[2] }}&period={{ entry[3] }}&period_type={{ entry[4] }}&submission_type=ADJUSTMENT">Create Adjustment</a><form method="POST" action="/soa/duplicate/{{ entry[0] }}" style="display:inline;"><button type="submit" class="btn btn-ghost">Mark Duplicate</button></form><a class="btn btn-ghost" href="/soa">Cancel</a></div></div></main></body></html>"""
VALIDATION_DASHBOARD_HTML = """<!DOCTYPE html><html><head>{{ base_style | safe }}<title>Validation Results</title></head><body><header><span class="logo">RI Platform</span><span class="divider">/</span><span class="title">Validation Results</span><a href="/" style="color:var(--muted);text-decoration:none;font-size:13px;margin-left:24px">Mapping Review</a><a href="/soa" style="color:var(--muted);text-decoration:none;font-size:13px;margin-left:16px">SOA Entry</a></header><main><div class="section"><h2>Latest Run Summary</h2>{% if runs %}<div class="card" style="display:block; padding:0;"><table style="width:100%; border-collapse:collapse; text-align:left;"><thead style="border-bottom:1px solid var(--border); font-family:var(--mono); font-size:11px; color:var(--muted); text-transform:uppercase;"><tr><th style="padding:12px 16px;">Run ID</th><th style="padding:12px 16px;">Date</th><th style="padding:12px 16px;">Total Checks</th><th style="padding:12px 16px;">Passed</th><th style="padding:12px 16px;">Failed</th><th style="padding:12px 16px;">Warned</th><th style="padding:12px 16px;">Skipped</th></tr></thead><tbody>{% for r in runs %}<tr style="border-bottom:1px solid var(--border);"><td style="padding:12px 16px;"><a href="/validation/{{ r.run_id }}" style="color:var(--accent); text-decoration:none; font-weight:600;">{{ r.run_id }}</a></td><td style="padding:12px 16px; font-size:12px;">{{ r.run_date.strftime('%Y-%m-%d %H:%M') if r.run_date else '—' }}</td><td style="padding:12px 16px;">{{ r.total_checks }}</td><td style="padding:12px 16px; color:var(--success);">{{ r.passed }}</td><td style="padding:12px 16px; color:{% if r.failed > 0 %}var(--danger){% else %}var(--success){% endif %}; font-weight:600;">{{ r.failed }}</td><td style="padding:12px 16px; color:var(--warn);">{{ r.warned }}</td><td style="padding:12px 16px; color:var(--muted);">{{ r.skipped }}</td></tr>{% endfor %}</tbody></table></div>{% else %}<p class="empty">No validation runs found.</p>{% endif %}</div><div class="section"><h2>Open Failures</h2>{% if failures %}{% for f in failures %}<div class="card"><div><div class="card-meta"><span class="tag tag-client">{{ f.client_id }}</span><span class="tag tag-cat">{{ f.treaty_id or 'NO_TREATY' }}</span><span class="tag tag-pending" style="background:#2a1e00; color:var(--danger);">{{ f.check_name }}</span></div><div class="card-col">{{ f.file_name }}</div><div class="card-samples">Failed Rows: {{ f.failed_count }} / {{ f.total_count }}</div><div class="card-created">Run ID: <a href="/validation/{{ f.run_id }}" style="color:var(--muted);">{{ f.run_id }}</a></div></div></div>{% endfor %}{% else %}<p class="empty">No open failures.</p>{% endif %}</div></main></body></html>"""
VALIDATION_RUN_HTML = """<!DOCTYPE html><html><head>{{ base_style | safe }}<title>Validation Run: {{ run_id }}</title></head><body><header><span class="logo">RI Platform</span><span class="divider">/</span><a href="/validation" style="color:var(--muted);text-decoration:none;font-size:13px">Validation Results</a><span class="divider">/</span><span class="title">{{ run_id }}</span></header><main>{% if grouped_results %}{% for group_key, checks in grouped_results.items() %}<div class="section"><div class="card-meta" style="margin-bottom: 12px;"><span class="tag tag-client">{{ group_key.client_id }}</span><span class="tag tag-cat">{{ group_key.treaty_id or 'NO_TREATY' }}</span><span class="tag tag-affects">{{ group_key.category }}</span><span style="font-family:var(--mono); font-size:13px; font-weight:600; margin-left:8px;">{{ group_key.file_name }}</span></div><div class="card" style="display:block; padding:0;"><table style="width:100%; border-collapse:collapse; text-align:left;"><thead style="border-bottom:1px solid var(--border); font-family:var(--mono); font-size:11px; color:var(--muted); text-transform:uppercase;"><tr><th style="padding:12px 16px;">Check Name</th><th style="padding:12px 16px;">Status</th><th style="padding:12px 16px;">Failed / Total</th><th style="padding:12px 16px;">Message</th></tr></thead><tbody>{% for check in checks %}<tr style="border-bottom:1px solid var(--border);"><td style="padding:12px 16px; font-family:var(--mono); font-size:12px; font-weight:600;">{{ check.check_name }}</td><td style="padding:12px 16px; font-family:var(--mono); font-size:12px; font-weight:600; color:{{ get_status_color(check.status) }};">{{ check.status }}</td><td style="padding:12px 16px; font-size:12px; font-family:var(--mono);">{% if check.status != 'SKIPPED' %}{{ check.failed_count }} / {{ check.total_count }}{% else %}—{% endif %}</td><td style="padding:12px 16px; font-size:12px; color:var(--muted);">{{ check.message or '' }}</td></tr>{% endfor %}</tbody></table></div></div>{% endfor %}{% else %}<div class="section"><p class="empty">No validation results found for this run.</p></div>{% endif %}</main></body></html>"""

# ==============================================================================
# ROUTING CONTROLLER IMPLEMENTATION
# ==============================================================================

@app.route("/")
def index():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, client_id, category, raw_col_name, sample_values, status, created_at
                FROM mapping_review_queue
                WHERE status = 'PENDING'
                ORDER BY created_at DESC
            """)
            pending = cur.fetchall()

            cur.execute("""
                SELECT id, client_id, category, raw_col_name, canonical_name,
                       status, de_resolved, de_resolved_by, reviewed_by, reviewed_at
                FROM mapping_review_queue
                WHERE status = 'AFFECTS_CALCULATIONS' AND de_resolved = FALSE
                ORDER BY created_at DESC
            """)
            needs_de = cur.fetchall()
            
    return render_template_string(INDEX_HTML, pending=pending, needs_de=needs_de, base_style=BASE_STYLE)


@app.route("/review/<int:item_id>", methods=["GET"])
def review(item_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, client_id, category, raw_col_name, sample_values
                FROM mapping_review_queue WHERE id = %s
            """, (item_id,))
            item = cur.fetchone()

    if not item:
        return "Mapping entity not found within standard ledger definitions.", 404
    return render_template_string(REVIEW_HTML, item=item, base_style=BASE_STYLE)


@app.route("/review/<int:item_id>", methods=["POST"])
def submit_review(item_id):
    action = request.form.get("action")
    canonical_name = request.form.get("canonical_name", "").strip()
    is_critical = request.form.get("is_critical") == "on"
    is_pii = request.form.get("is_pii") == "on"
    reviewed_by = request.form.get("reviewed_by", "").strip()
    now = datetime.now(timezone.utc)

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT client_id, category, raw_col_name
                FROM mapping_review_queue WHERE id = %s
            """, (item_id,))
            row = cur.fetchone()
            if not row:
                return "Entity missing from verification bounds.", 404
            client_id, category, raw_col_name = row[0], row[1], row[2]

            if action in ("approve", "affects_calculations"):
                cur.execute("""
                    SELECT COALESCE(MAX(mapping_version), 0) + 1
                    FROM column_mapping
                    WHERE client_id = %s AND category = %s
                """, (client_id, category))
                next_version = cur.fetchone()[0]

                cur.execute("""
                    INSERT INTO column_mapping
                        (client_id, category, raw_col_name, canonical_name,
                         is_critical, is_pii, mapping_version,
                         effective_from, created_by, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (client_id, category, raw_col_name, canonical_name,
                      is_critical, is_pii, next_version,
                      now.date(), reviewed_by, now))

                status = "APPROVED" if action == "approve" else "AFFECTS_CALCULATIONS"

                cur.execute("""
                    UPDATE mapping_review_queue
                    SET status         = %s,
                        canonical_name = %s,
                        is_critical    = %s,
                        is_pii         = %s,
                        reviewed_by    = %s,
                        reviewed_at    = %s
                    WHERE id = %s
                """, (status, canonical_name, is_critical, is_pii, reviewed_by, now, item_id))

            elif action == "reject":
                cur.execute("""
                    UPDATE mapping_review_queue
                    SET status      = 'REJECTED',
                        reviewed_by = %s,
                        reviewed_at = %s
                    WHERE id = %s
                """, (reviewed_by, now, item_id))
            conn.commit()

    return redirect(url_for("index"))


@app.route("/resolve/<int:item_id>", methods=["POST"])
def resolve(item_id):
    de_name = request.form.get("de_name", "").strip()
    now = datetime.now(timezone.utc)

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE mapping_review_queue
                SET de_resolved    = TRUE,
                    de_resolved_by = %s,
                    de_resolved_at = %s
                WHERE id = %s
            """, (de_name, now, item_id))
            conn.commit()

    return redirect(url_for("index"))


def get_soa_entry(cur, entry_id):
    """Fetches full transaction payload context for a target record entry ID."""
    cur.execute("""
        SELECT id, client_id, year, period, period_type,
               premium_soa, claims_soa, commission_soa, tax_soa, net_soa,
               additional_items, entered_by, version, entered_at, status
        FROM soa_entries
        WHERE id = %s
    """, (entry_id,))
    return cur.fetchone()


def get_soa_dashboard_data():
    """Compiles operational state aggregations parameterized dynamically."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT
                    client_id,
                    year,
                    quarter AS period
                FROM file_tracking
                WHERE uploaded_at >= CURRENT_DATE
                  AND uploaded_at < CURRENT_DATE + INTERVAL '1 day'
                ORDER BY client_id, year, quarter
            """)
            periods = cur.fetchall()

            action_required = []
            pending_approval = []
            approved = []
            reconciled = []
            adjustment_candidates = []

            for row in periods:
                client_id, year, period = row['client_id'], row['year'], row['period']
                
                cur.execute("""
                    SELECT
                        id, client_id, year, period, period_type,
                        premium_soa, claims_soa, commission_soa, tax_soa, net_soa,
                        additional_items, entered_by, version, entered_at,
                        status, approved_by, approved_at
                    FROM soa_entries
                    WHERE client_id = %s
                      AND year = %s
                      AND period = %s
                    ORDER BY version DESC
                    LIMIT 1
                """, (client_id, year, period))

                soa = cur.fetchone()

                if not soa:
                    action_required.append({
                        "client_id": client_id,
                        "year": year,
                        "period": period,
                        "status": "READY_FOR_SOA_ENTRY",
                        "version": None
                    })
                    continue

                status = soa['status']

                if status == "REJECTED":
                    action_required.append(dict(soa))
                elif status == "PENDING_APPROVAL":
                    pending_approval.append(dict(soa))
                elif status == "APPROVED":
                    approved.append(dict(soa))
                elif status == "RECONCILED":
                    reconciled.append(dict(soa))
                    adjustment_candidates.append(dict(soa))

    return {
        "action_required": action_required,
        "pending_approval": pending_approval,
        "approved": approved,
        "reconciled": reconciled,
        "adjustment_candidates": adjustment_candidates
    }

@app.route("/soa")
def soa_dashboard():
    data = get_soa_dashboard_data()
    return render_template_string(
        SOA_DASHBOARD_HTML,
        action_required=data["action_required"],
        pending_approval=data["pending_approval"],
        approved=data["approved"],
        reconciled=data["reconciled"],
        adjustment_candidates=data["adjustment_candidates"],
        base_style=BASE_STYLE
    )


@app.route("/soa/enter", methods=["GET", "POST"])
def soa_enter():
    if request.method == "GET":
        return render_template_string(
            SOA_ENTRY_HTML,
            client_id=request.args.get("client_id", "").strip(),
            year=request.args.get("year", "").strip(),
            period=request.args.get("period", "").strip(),
            period_type=request.args.get("period_type", "quarterly").strip(),
            error=None,
            base_style=BASE_STYLE,
        )

    form_values = {
        "client_id": request.form.get("client_id", "").strip(),
        "year": request.form.get("year", "").strip(),
        "period": request.form.get("period", "").strip(),
        "period_type": request.form.get("period_type", "quarterly").strip(),
    }

    try:
        premium_soa = Decimal(request.form.get("premium_soa", ""))
        claims_soa = Decimal(request.form.get("claims_soa", ""))
        commission_soa = Decimal(request.form.get("commission_soa", ""))
        tax_soa = Decimal(request.form.get("tax_soa", ""))
        entered_by = request.form.get("entered_by", "").strip()
        additional_items = json.loads(request.form.get("additional_items_json", "[]"))

        if not all(form_values.values()) or not entered_by:
            raise ValueError("All SOA entry fields are required.")
        if not isinstance(additional_items, list):
            raise ValueError("Additional items must be formatted as a valid list array context.")

        net_soa = premium_soa - claims_soa - commission_soa - tax_soa
        clean_items = []
        for item in additional_items:
            label = str(item.get("label", "")).strip()
            direction = str(item.get("direction", "")).strip().lower()
            amount = Decimal(str(item.get("amount", "0")))

            if not label:
                raise ValueError("An explicit item label descriptor must be declared.")
            if direction not in ("credit", "debit"):
                raise ValueError("Direction must map cleanly to 'credit' or 'debit' rules.")
            if amount < 0:
                raise ValueError("Additional item amounts cannot be signed as negative variants.")

            clean_items.append({
                "label": label,
                "amount": float(amount),
                "direction": direction,
            })
            net_soa += amount if direction == "credit" else -amount

    except (InvalidOperation, json.JSONDecodeError, TypeError, ValueError) as exc:
        return render_template_string(
            SOA_ENTRY_HTML,
            **form_values,
            error=str(exc),
            base_style=BASE_STYLE,
        ), 400

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(MAX(version),0)+1
                FROM soa_entries
                WHERE client_id=%s AND year=%s AND period=%s
            """, (form_values["client_id"], form_values["year"], form_values["period"]))
            version = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO soa_entries (
                    client_id, year, period, period_type,
                    premium_soa, claims_soa, commission_soa, tax_soa, net_soa,
                    additional_items, status, entered_by, version
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING_APPROVAL', %s, %s)
            """, (
                form_values["client_id"], form_values["year"],
                form_values["period"], form_values["period_type"], 
                premium_soa, claims_soa, commission_soa, tax_soa, net_soa,
                Json(clean_items), entered_by, version
            ))
            conn.commit()

    return redirect(url_for("soa_dashboard"))


@app.route("/soa/approve/<int:entry_id>", methods=["GET", "POST"])
def soa_approve(entry_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            entry = get_soa_entry(cur, entry_id)

            if not entry:
                return "Target SOA registration missing.", 404

            if request.method == "GET":
                return render_template_string(
                    SOA_APPROVE_HTML,
                    entry=entry,
                    additional_items=entry[10] or [],
                    error=None,
                    base_style=BASE_STYLE,
                )

            approved_by = request.form.get("approved_by", "").strip()
            action = request.form.get("action", "").strip()
            rejection_reason = request.form.get("rejection_reason", "").strip()

            try:
                if not approved_by:
                    raise ValueError("Approved By descriptor cannot be submitted empty.")

                if action == "approve":
                    cur.execute("""
                        UPDATE soa_entries
                        SET status = 'APPROVED', approved_by = %s, approved_at = NOW()
                        WHERE id = %s
                    """, (approved_by, entry_id))
                elif action == "reject":
                    if not rejection_reason:
                        raise ValueError("Rejection reasons are mandatory constraints.")
                    cur.execute("""
                        UPDATE soa_entries
                        SET status = 'REJECTED', rejection_reason = %s
                        WHERE id = %s
                    """, (rejection_reason, entry_id))
                else:
                    raise ValueError("Unknown approval payload signature.")
                conn.commit()

            except psycopg2.errors.CheckViolation:
                conn.rollback()
                # Refetch fresh database copy to populate HTML context accurately
                entry = get_soa_entry(cur, entry_id)
                return render_template_string(
                    SOA_APPROVE_HTML,
                    entry=entry,
                    additional_items=entry[10] or [],
                    error="Segregation of Duties Violation: Approving operator cannot be identical to entering actor.",
                    base_style=BASE_STYLE,
                ), 400
            except ValueError as exc:
                conn.rollback()
                return render_template_string(
                    SOA_APPROVE_HTML,
                    entry=entry,
                    additional_items=entry[10] or [],
                    error=str(exc),
                    base_style=BASE_STYLE,
                ), 400

    return redirect(url_for("soa_dashboard"))


@app.route("/soa/adjustment/<int:soa_id>")
def soa_adjustment(soa_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            entry = get_soa_entry(cur, soa_id)
    
    if not entry:
        return "Adjustment candidate lookup failure.", 404
        
    return render_template_string(SOA_ADJUSTMENT_HTML, entry=entry, base_style=BASE_STYLE)


@app.route("/soa/duplicate/<int:soa_id>", methods=["POST"])
def soa_duplicate(soa_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE soa_entries
                SET status = 'DUPLICATE'
                WHERE id = %s
            """, (soa_id,))
            conn.commit()
            
    return redirect(url_for("soa_dashboard"))


# ==============================================================================
# QUALITY VALIDATION MONITORING CORE LOGIC
# ==============================================================================

def get_status_color(status):
    """Resolves operational engine outcomes into dynamic UI hexadecimal colors."""
    return {
        "PASS":    "var(--success)",
        "FAIL":    "var(--danger)",
        "WARN":    "var(--warn)",
        "SKIPPED": "var(--muted)"
    }.get(status, "var(--text)")


@app.route("/validation")
def validation_dashboard():
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    run_id,
                    MIN(checked_at) AS run_date,
                    COUNT(*) AS total_checks,
                    COUNT(*) FILTER (WHERE status = 'PASS') AS passed,
                    COUNT(*) FILTER (WHERE status = 'FAIL') AS failed,
                    COUNT(*) FILTER (WHERE status = 'WARN') AS warned,
                    COUNT(*) FILTER (WHERE status = 'SKIPPED') AS skipped
                FROM validation_results
                GROUP BY run_id
                ORDER BY MIN(checked_at) DESC
                LIMIT 10
            """)
            runs = cur.fetchall()

            cur.execute("""
                SELECT
                    client_id, treaty_id, file_name,
                    check_name, failed_count, total_count, run_id
                FROM validation_results
                WHERE status = 'FAIL'
                ORDER BY checked_at DESC
                LIMIT 100
            """)
            failures = cur.fetchall()

    return render_template_string(
        VALIDATION_DASHBOARD_HTML,
        runs=runs,
        failures=failures,
        base_style=BASE_STYLE
    )


@app.route("/validation/<run_id>")
def validation_run(run_id):
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    client_id, treaty_id, file_name, category,
                    check_name, status, failed_count,
                    total_count, message, checked_at
                FROM validation_results
                WHERE run_id = %s
                ORDER BY file_name, check_name
            """, (run_id,))
            raw_results = cur.fetchall()

    # Pre-defined structural rule check sort weight matrix
    check_order = [
        "TREATY_MAPPING", "SOA_RECONCILIATION_PREMIUM", "SOA_RECONCILIATION_CLAIMS",
        "DUPLICATE_PREMIUM", "DUPLICATE_CLAIMS", "NEW_BUSINESS_FLAG",
        "RISAR_VALIDATION", "EFFECTIVE_DATE", "AGE_RANGE", "PRODUCT_CODE",
        "RATE_TABLE", "PREMIUM_EXISTS", "RISAR_GTE_CLAIM", "DOL_COVERAGE",
        "LAPSE_CHECK", "NO_ZERO_NEGATIVE", "CAL_CHECK"
    ]

    def get_sort_key(check_row):
        try:
            return check_order.index(check_row['check_name'])
        except ValueError:
            return 999

    GroupKey = collections.namedtuple('GroupKey', ['client_id', 'treaty_id', 'file_name', 'category'])
    grouped_results = collections.defaultdict(list)

    for row in raw_results:
        key = GroupKey(row['client_id'], row['treaty_id'], row['file_name'], row['category'])
        grouped_results[key].append(row)

    for key in grouped_results:
        grouped_results[key].sort(key=get_sort_key)

    return render_template_string(
        VALIDATION_RUN_HTML,
        run_id=run_id,
        grouped_results=grouped_results,
        get_status_color=get_status_color,
        base_style=BASE_STYLE
    )


if __name__ == "__main__":
    # Internal dev server parameters; deployment topologies should mount this via WSGI / Gunicorn.
    app.run(host="0.0.0.0", port=5000, debug=True)