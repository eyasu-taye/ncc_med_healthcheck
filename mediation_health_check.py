import re
from collections import defaultdict
from prettytable import PrettyTable

# --- Read log text safely ---
with open("MED_HC_11252025.txt", encoding="utf-8", errors="ignore") as f:
    text = f.read()

results = []

# --- Cluster Stack ---
if re.search(r'pacemaker|corosync', text, re.I):
    results.append(["Cluster Stack", "✅ Active", "Pacemaker & Corosync running fine"])
else:
    results.append(["Cluster Stack", "❌ Inactive", "Cluster services not running"])

# --- Node Health ---
nodes = re.findall(r'(?:node\d+|10\.\d+\.\d+\.\d+)', text)
unique_nodes = sorted(set(nodes))
count_online = len(unique_nodes)

if unique_nodes:
    results.append([
        "Node Health",
        f"✅ {count_online} node(s) online",
        ", ".join(unique_nodes)
    ])
else:
    results.append([
        "Node Health",
        "⚠️ Unknown",
        "No node info found"
    ])

# --- Resources ---
if re.search(r'Started|running', text, re.I):
    results.append(["Resources", "✅ All started", "Including VIPs and LVs"])
else:
    results.append(["Resources", "⚠️ Some stopped", "Check resource list"])

# --- Storage ---
node_blocks = re.split(r'(?=\b(?:node\d+|10\.\d+\.\d+\.\d+)\b)', text)
high_usage_overall = []

for block in node_blocks:
    node_match = re.search(r'(node\d+|10\.\d+\.\d+\.\d+)', block)
    node_name = node_match.group(1) if node_match else "Unknown"

    # Find mount points and usage %, ignoring tmpfs/devtmpfs
    disk_usage = re.findall(r'(\S+)\s+\d+\S*\s+\d+\S*\s+\d+\S*\s+(\d+)%', block)
    for m, u in disk_usage:
        if m.startswith("/dev"):
            usage = int(u)
            if usage > 85:
                high_usage_overall.append((node_name, m, usage))

if high_usage_overall:
    nodes_high = sorted(set(node for node, _, _ in high_usage_overall))
    count_nodes = len(nodes_high)
    mounts_summary = ", ".join([f"{m} {u}% on {node}" for node, m, u in high_usage_overall])
    results.append([
        "Storage",
        f"⚠️ {count_nodes} node(s) above 85%",
        mounts_summary
    ])
else:
    results.append(["Storage", "✅ OK", "All mounts below 85% usage"])

# --- Historical Issues (Fence Failures) ---
failure_count = defaultdict(int)
failure_nodes = defaultdict(list)
pattern = r"reboot of (\S+) failed"

for line in text.splitlines():
    match = re.search(pattern, line)
    if match:
        node = match.group(1)
        failure_count[node] += 1
        failure_nodes[node].append(line.strip())

if failure_count:
    total_failures = sum(failure_count.values())
    nodes_summary = ", ".join(f"{node} ({count})" for node, count in failure_count.items())
    results.append([
        "Historical Issues",
        f"⚠️ {total_failures} failure(s)",
        nodes_summary
    ])
else:
    results.append([
        "Historical Issues",
        "✅ Clean",
        "No recent failures"
    ])

# --- Missing Package ---
if re.search(r'missing\s+pcs', text, re.I):
    results.append(["Missing Package", "⚠️ pcs missing", "Reinstall pcs if needed"])
else:
    results.append(["Missing Package", "✅ Installed", "All required packages present"])

# --- STEP 3: Display Results as HTML Table ---
html_table = "<table>\n"
html_table += "  <tr><th>Category</th><th>Status</th><th>Remarks</th></tr>\n"
for row in results:
    html_table += "  <tr>\n"
    for item in row:
        html_table += f"    <td>{item}</td>\n"
    html_table += "  </tr>\n"
html_table += "</table>"

from IPython.display import display, HTML
display(HTML(html_table))