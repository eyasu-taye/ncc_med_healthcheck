import re
from datetime import datetime, timedelta
from prettytable import PrettyTable

# --- Load the log file safely ---
with open("mdc1_hc_11252025.txt", encoding="utf-8", errors="ignore") as f:
    log_data = f.read()

results = []

def add_row(category, status, remarks):
    results.append([category.strip(), status.strip(), remarks.strip()])

# --- STEP 1: Split sections by markers ---
split_pattern = r"(#+\n#\s*[A-Za-z0-9_\-/ ]+?\s*#\n#+)"
parts = re.split(split_pattern, log_data)

sections = []
for i in range(1, len(parts), 2):
    header = parts[i]
    content = parts[i + 1] if i + 1 < len(parts) else ""
    name = re.search(r"#\s*([A-Za-z0-9_\-/ ]+?)\s*#", header)
    section_name = name.group(1).strip().lower() if name else "unknown"
    sections.append((section_name, content.strip()))

# --- STEP 2: Analyze each section ---
for section_name, content in sections:
    up_items = re.findall(r"([A-Za-z0-9_\-:.]+).*?\bUP\b", content)
    down_items = re.findall(r"([A-Za-z0-9_\-:.]+).*?\b(DOWN|FAIL|ERROR)\b", content, re.I)

       # --- SPS STATUS ---
    if "sps-status" in section_name:
        up_items = re.findall(r"([A-Za-z0-9_\-:.]+).*?\bUP\b", content)
        down_items = re.findall(r"([A-Za-z0-9_\-:.]+).*?\b(DOWN|FAIL|ERROR)\b", content, re.I)
        down_list = ", ".join(set(d[0] for d in down_items)) if down_items else "None"
        add_row("SPS Status",
                "⚠️ Issues" if down_items else "✅ Stable",
                f"{len(up_items)} UP, {len(down_items)} DOWN ({down_list})")

    # --- DB STATS ---
    elif "db-stats" in section_name:
        total_nodes = re.search(r"Total DB nodes:\s+(\d+)", content)
        responding_nodes = re.search(r"DB nodes responding:\s+(\d+)", content)
        record_sets = len(re.findall(r'^[A-Za-z0-9_]+\s+\d', content, re.M))

        total_nodes = int(total_nodes.group(1)) if total_nodes else 0
        responding_nodes = int(responding_nodes.group(1)) if responding_nodes else 0
        add_row("DB Stats",
                "✅ OK" if total_nodes == responding_nodes else "⚠️ Issue",
                f"{responding_nodes}/{total_nodes} nodes responding, {record_sets} record sets found")

    # --- XDR STATUS ---
    elif re.search(r'\bxdr status\b', section_name) and "pod" not in section_name:
        lag = re.findall(r"lag[:\s]+(\d+)", content)
        latency = re.findall(r"latency_ms[:\s]+(\d+)", content)
        state = re.search(r"Service State:\s*(\w+)", content)
        avg_lag = sum(map(int, lag)) / len(lag) if lag else 0
        avg_lat = sum(map(int, latency)) / len(latency) if latency else 0
        state_val = state.group(1) if state else "UNKNOWN"
        status = "✅ OK" if state_val.upper() == "UP" and avg_lag < 500 else "⚠️ High Lag/Down"
        add_row("XDR Status", status,
                f"State: {state_val}, Avg lag {avg_lag:.1f} ms, Avg latency {avg_lat:.1f} ms")

    # --- POD XDR STATUS ---
    elif "pod xdr" in section_name or ("xdr status" in section_name and "pod" in section_name):
        pods = re.findall(r"(\S+)\s+lag[:=]\s*(\d+).*?latency[:=]\s*(\d+)", content)
        total_pods = len(pods)
        high_lag = [(p, l, t) for p, l, t in pods if int(l) > 1000 or int(t) > 1000]
        avg_lag = sum(int(l) for _, l, _ in pods)/total_pods if total_pods else 0
        if total_pods:
            status = "⚠️ High Lag" if high_lag else "✅ Normal"
            top = ", ".join([f"{p}(lag {l}, lat {t})" for p, l, t in high_lag[:3]]) if high_lag else "All normal"
            add_row("Pod XDR Status", status, f"Pods: {total_pods}, Avg lag: {avg_lag:.1f}, {top}")
        else:
            add_row("Pod XDR Status", "ℹ️ No Data", "No pod lag/latency entries")

    # --- AEROSPIKE RESTART STATUS ---
    elif "restart" in section_name and "aerospike" in section_name:
        now = datetime.now()
        restarts = []
        for pod, t in re.findall(r'(\S+)\s+(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', content):
            try:
                restart_time = datetime.strptime(t, "%Y-%m-%dT%H:%M:%S")
                if (now - restart_time) <= timedelta(hours=24):
                    restarts.append((pod, t))
            except ValueError:
                pass
        if restarts:
            add_row("Aerospike Restart Status",
                    f"⚠️ {len(restarts)} Found",
                    ", ".join([f"{p} @ {t}" for p, t in restarts[:5]]) + (" ..." if len(restarts) > 5 else ""))
        else:
            add_row("Aerospike Restart Status", "✅ None", "No recent restarts")


    # Diameter Peers
    elif "diameter" in section_name:
        peers = re.findall(r"(SS7|PGW|DRA).*?(UP|DOWN)", content)
        total = len(peers)
        down_peers = [p[0] for p in peers if p[1].lower() == "down"]
        add_row("Diameter Peers",
                "⚠️ Some Down" if down_peers else "✅ All UP",
                f"Total: {total}, Down: {len(down_peers)} ({', '.join(down_peers) if down_peers else 'None'})")

    # Replication Status
    elif "replication" in section_name:
        ok = len(re.findall(r"Running.*Yes", content, re.I))
        failed = len(re.findall(r"(No|Error)", content, re.I))
        add_row("Replication Status",
                "⚠️ Issue" if failed else "✅ OK",
                f"Running: {ok}, Failed: {failed}")

    # Pod Restarts (24h)
    elif "restart" in section_name:
        now = datetime.now()
        restarts = []
        for pod, t in re.findall(r'(\S+)\s+(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', content):
            try:
                restart_time = datetime.strptime(t, "%Y-%m-%dT%H:%M:%S")
                if (now - restart_time) <= timedelta(hours=24):
                    restarts.append((pod, t))
            except ValueError:
                pass
        if restarts:
            add_row("Pod Restarts (24h)",
                    f"⚠️ {len(restarts)} Found",
                    ", ".join([f"{p[0]} @ {p[1]}" for p in restarts[:100]]) + (" ..." if len(restarts) > 100 else ""))
        else:
            add_row("Pod Restarts (24h)", "✅ None", "No recent restarts")

    # Node Status
    elif "node status" in section_name:
        nodes = re.findall(r"(mdc\S+).*?(Ready|NotReady)", content)
        ready = [n for n in nodes if "Ready" in n[1]]
        not_ready = [n for n in nodes if "NotReady" in n[1]]
        add_row("Node Status",
                "⚠️ Not Ready" if not_ready else "✅ All Ready",
                f"Total: {len(nodes)}, Ready: {len(ready)}, NotReady: {len(not_ready)} ({', '.join([n[0] for n in not_ready])})")

    # Pod CPU Status
    elif "pod cpu status" in section_name:
        rows = re.findall(r"(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\d+)m\s+(\d+)", content)
        cpu_vals = [int(r[4]) for r in rows]
        mem_vals = [int(r[5]) for r in rows]
        avg_cpu = sum(cpu_vals)/len(cpu_vals) if cpu_vals else 0
        avg_mem = sum(mem_vals)/len(mem_vals) if mem_vals else 0
        high_cpu = [(r[1], r[4]) for r in rows if int(r[4]) > avg_cpu]
        high_mem = [(r[1], r[5]) for r in rows if int(r[5]) > avg_mem]
        add_row("Pod CPU/Memory Usage",
                "✅ Normal" if not high_cpu and not high_mem else "⚠️ High Usage",
                f"Pods: {len(rows)}, Above Avg CPU: {len(high_cpu)}, Above Avg MEM: {len(high_mem)}")

    # Flush-max-ms
    elif "flush-max-ms" in section_name:
        flush = [int(v) for v in re.findall(r"(\d+)", content)]
        high = [v for v in flush if v > 1000]
        add_row("Flush-Max-MS", "⚠️ High Latency" if high else "✅ Normal",
                f"Count >1000ms: {len(high)}, Max: {max(flush) if flush else 0}ms")

    # Check Clock Skew AS
    elif "clock_skew_as" in section_name:
        skews = [int(v) for v in re.findall(r"skew-ms\s+(\d+)", content)]
        avg_skew = sum(skews)/len(skews) if skews else 0
        max_skew = max(skews) if skews else 0
        add_row("Clock Skew (AS)",
                "⚠️ High Skew" if max_skew > 100 else "✅ Normal",
                f"Avg: {avg_skew:.1f} ms, Max: {max_skew} ms, Samples: {len(skews)}")

    # Backup Status
    elif "check_backup_status" in section_name or "check_btel_backup_status" in section_name:
        total = re.search(r"total\s+([\d.]+[KMG]?)", content)
        dirs = re.findall(r"^drwx", content, re.M)
        add_row("Backup Status", "✅ OK", f"Dirs: {len(dirs)}, Size: {total.group(1) if total else 'Unknown'}")

    # Disk Space
    elif "diskspace" in section_name:
        disks = re.findall(r"(/\S+)\s+\d+\S*\s+\d+\S*\s+\d+\S*\s+(\d+)%", content)
        if disks:
            high_usage = [(m, int(u)) for m, u in disks if int(u) > 85]
            if high_usage:
                mounts_str = ", ".join([f"{m}: {u}%" for m, u in high_usage])
                add_row(section_name,
                        f"⚠️ {len(high_usage)} Mounts >85%",
                        mounts_str)
            else:
                add_row(section_name, "✅ OK", "All mounts below 85%")
        else:
            add_row(section_name, "ℹ️ No Data", "No mount info found")

    # A/C Device Dump Status
    elif "check_a/c_device_dump_status" in section_name:
        folders = re.findall(r"^drwx", content, re.M)
        total_size = re.search(r"total\s+([\d.]+[KMG]?)", content)
        size_str = total_size.group(1) if total_size else "Unknown"
        add_row("A/C Device Dump Status", "✅ OK", f"Folders: {len(folders)}, Total Size: {size_str}")

    # EDR / CDR Generation
    elif "edr_status" in section_name or "cdr_status" in section_name:
        files = re.findall(r"\.EDR\.gz|\.CDR\.gz", content)
        total_size = re.search(r"total\s+([\d.]+[KMG]?)", content)
        size_str = total_size.group(1) if total_size else "Unknown"
        add_row(
            "EDR Generation" if "edr" in section_name else "CDR Generation",
            "✅ OK" if files else "⚠️ None Found",
            f"Files: {len(files)}, Size: {size_str}"
        )

    # Cron Jobs
    elif "cronjob" in section_name:
        jobs = re.findall(r"^\S+\s+\S+\s+\S+\s+(True|False)", content, re.M)
        suspended = [j for j in jobs if j.lower() == "true"]
        add_row("Cron Jobs",
                "⚠️ Suspended Jobs" if suspended else "✅ OK",
                f"Total: {len(jobs)}, Suspended: {len(suspended)}")

    else:
        add_row(section_name, "ℹ️ Parsed", f"{len(up_items)} UP, {len(down_items)} DOWN")

# --- STEP 3: Display Results as HTML Table ---
html_table = "<table>\n"
html_table += "  <tr><th>Section</th><th>Status</th><th>Remarks</th></tr>\n"
for row in results:
    html_table += "  <tr>\n"
    for item in row:
        html_table += f"    <td>{item}</td>\n"
    html_table += "  </tr>\n"
html_table += "</table>"

from IPython.display import display, HTML
display(HTML(html_table))