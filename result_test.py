import mysql.connector

DB_CONFIG = {
    'user': 'root', 'password': '', 'host': 'localhost',
    'database': 'dashboard_dxx',
}


def safe_div(n, d):
    return round((n / d) * 100, 2) if d and d > 0 else 0.0

def calc_sla(sla_code, ans_in, answered, offered, abd_in):
    """SLA1: ans_in/(offered-abd_in), SLA2: ans_in/answered, SLA3: 1-(ans_out/(offered-abd_60))"""
    if sla_code == 'SLA2':
        return safe_div(ans_in, answered)
    elif sla_code == 'SLA3':
        ans_out = max(0, answered - ans_in)
        denom = max(0, offered - abd_in)
        return round((1 - ans_out / denom) * 100, 2) if denom > 0 else 0.0
    else:  # SLA1
        denom = max(0, offered - abd_in)
        return safe_div(ans_in, denom)

def calc_abd(abd_code, offered, answered, abd_in, abd_60):
    """
    Abd1: 1 - (abd_out / offered)              → high is good
    Abd2: abd_out / (offered - abd_in)
    Abd3: abd_out / answered
    Abd4: abd_out / offered
    Abd5: 1 - (abd_after_60 / (offered-abd_in)) → high is good
    """
    if abd_code is None or abd_code in ('NONE', ''):
        return 0.0
    abd_raw = max(0, offered - answered)
    abd_out = max(0, abd_raw - abd_in)

    if abd_code == 'Abd1':
        return round((1 - abd_out / offered) * 100, 2) if offered > 0 else 0.0
    elif abd_code == 'Abd2':
        denom = max(0, offered - abd_in)
        return safe_div(abd_out, denom)
    elif abd_code == 'Abd3':
        return safe_div(abd_out, answered)
    elif abd_code == 'Abd4':
        return safe_div(abd_out, offered)
    elif abd_code == 'Abd5':
        abd_out_60 = max(0, abd_raw - abd_60)
        denom = max(0, offered - abd_in)
        return round((1 - abd_out_60 / denom) * 100, 2) if denom > 0 else 0.0
    else:
        return safe_div(abd_raw, offered)


def run():
    cnx = mysql.connector.connect(**DB_CONFIG)
    cur = cnx.cursor(dictionary=True)

    # ── 1) Overview ──────────────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) AS n FROM companies")
    n_comp = cur.fetchone()['n']
    cur.execute("SELECT COUNT(*) AS n FROM queue_config")
    n_queues = cur.fetchone()['n']
    cur.execute("SELECT COUNT(*) AS n FROM call_metrics_history")
    n_rows = cur.fetchone()['n']
    cur.execute("SELECT MIN(interval_start) AS mn, MAX(interval_start) AS mx FROM call_metrics_history")
    rng = cur.fetchone()

    print("=" * 100)
    print("  dashboard_dxx — ELT Results")
    print("=" * 100)
    print(f"  Companies : {n_comp}")
    print(f"  Queues    : {n_queues}")
    print(f"  Metrics   : {n_rows:,} rows")
    print(f"  Range     : {rng['mn']}  →  {rng['mx']}")

    # ── 2) Per-company summary ───────────────────────────────────────────────
    cur.execute("""
        SELECT
            c.name                          AS company,
            qc.sla_formula,
            qc.abd_formula,
            qc.target_ans_pct,
            qc.target_abd_pct,
            COUNT(DISTINCT qc.id)           AS queues,
            SUM(m.offered)                  AS offered,
            SUM(m.answered)                 AS answered,
            SUM(m.abandoned)                AS abandoned,
            SUM(m.ans_in_sla)               AS ans_in_sla,
            SUM(m.ans_out_sla)              AS ans_out_sla,
            SUM(m.abd_in_sla)               AS abd_in_sla,
            SUM(m.abd_out_sla)              AS abd_out_sla,
            SUM(m.abd_in_60)                AS abd_in_60,
            -- weighted time metrics
            ROUND(SUM(m.avg_handle_time * m.answered) / NULLIF(SUM(m.answered), 0), 1)  AS avg_aht,
            ROUND(SUM(m.avg_answer_time * m.answered) / NULLIF(SUM(m.answered), 0), 1)  AS avg_asa,
            ROUND(SUM(m.hold_time_total)              / NULLIF(SUM(m.answered), 0), 1)  AS avg_hold
        FROM call_metrics_history m
        JOIN queue_config qc ON qc.id = m.queue_id
        JOIN companies c     ON c.id  = qc.company_id
        GROUP BY c.id, c.name, qc.sla_formula, qc.abd_formula,
                 qc.target_ans_pct, qc.target_abd_pct
        ORDER BY c.name
    """)
    rows = cur.fetchall()

    hdr = (f"  {'Company':<18} {'Qs':>3} {'Offered':>8} {'Ans':>8} {'Abd':>6}"
           f"  {'AnsSLA':>6} {'AnsOut':>6} {'AbdSLA':>6} {'AbdOut':>6}"
           f"  {'SLA%':>7} {'Abd%':>7}"
           f"  {'AHT':>6} {'ASA':>6} {'Hold':>6} {'TTC':>6}")
    print(f"\n{'─'*len(hdr)+''}")
    print(hdr)
    print(f"{'─'*len(hdr)+''}")

    for r in rows:
        offered   = r['offered'] or 0
        answered  = r['answered'] or 0
        abandoned = r['abandoned'] or 0
        ans_sla   = r['ans_in_sla'] or 0
        ans_out   = r['ans_out_sla'] or 0
        abd_sla   = r['abd_in_sla'] or 0
        abd_out   = r['abd_out_sla'] or 0
        abd_60    = r['abd_in_60'] or 0

        sla_code = r['sla_formula'] or 'SLA1'
        abd_code = r['abd_formula'] or 'NONE'

        # Apply correct formulas
        sla_pct = calc_sla(sla_code, ans_sla, answered, offered, abd_sla)
        abd_pct = calc_abd(abd_code, offered, answered, abd_sla, abd_60)

        aht  = r['avg_aht']  or 0
        asa  = r['avg_asa']  or 0
        hold = r['avg_hold'] or 0
        ttc  = round(aht + asa, 1)

        def fmt(v): return f"{v:.0f}s" if v else "  -"

        # Mark high-is-good formulas with ↑
        abd_mark = '↑' if abd_code in ('Abd1', 'Abd5') else ' '

        print(f"  {r['company']:<18} {r['queues']:>3} {offered:>8,} {answered:>8,} {abandoned:>6,}"
              f"  {ans_sla:>6,} {ans_out:>6,} {abd_sla:>6,} {abd_out:>6,}"
              f"  {sla_pct:>6.1f}% {abd_pct:>5.1f}%{abd_mark}"
              f"  {fmt(aht):>6} {fmt(asa):>6} {fmt(hold):>6} {fmt(ttc):>6}")

        tgt_a = f"{r['target_ans_pct']}%" if r['target_ans_pct'] else '-'
        tgt_d = f"{r['target_abd_pct']}%" if r['target_abd_pct'] else '-'
        print(f"    {sla_code}/{abd_code}  Target: ans≥{tgt_a}  abd{'≥' if abd_code in ('Abd1','Abd5') else '≤'}{tgt_d}")

    # ── 3) Language breakdown ────────────────────────────────────────────────
    cur.execute("""
        SELECT
            COALESCE(qc.language, 'Unknown') AS lang,
            COUNT(DISTINCT qc.id)            AS queues,
            SUM(m.offered)                   AS offered,
            SUM(m.answered)                  AS answered
        FROM call_metrics_history m
        JOIN queue_config qc ON qc.id = m.queue_id
        GROUP BY qc.language
        ORDER BY offered DESC
    """)
    langs = cur.fetchall()

    print(f"\n{'─'*60}")
    print("  Language Breakdown")
    print(f"{'─'*60}")
    print(f"  {'Language':<15} {'Queues':>6} {'Offered':>10} {'Answered':>10}")
    for l in langs:
        print(f"  {l['lang']:<15} {l['queues']:>6} {(l['offered'] or 0):>10,} {(l['answered'] or 0):>10,}")

    # ── 4) Top 10 queues by volume ───────────────────────────────────────────
    cur.execute("""
        SELECT
            c.name AS company,
            qc.queue_name,
            qc.language,
            SUM(m.offered)    AS offered,
            SUM(m.answered)   AS answered,
            SUM(m.abandoned)  AS abandoned,
            SUM(m.ans_in_sla) AS ans_sla
        FROM call_metrics_history m
        JOIN queue_config qc ON qc.id = m.queue_id
        JOIN companies c     ON c.id  = qc.company_id
        GROUP BY qc.id, c.name, qc.queue_name, qc.language
        ORDER BY offered DESC
        LIMIT 10
    """)
    top = cur.fetchall()

    print(f"\n{'─'*80}")
    print("  Top 10 Queues by Volume")
    print(f"{'─'*80}")
    print(f"  {'Company':<16} {'Queue':<30} {'Lang':<8} {'Offered':>8} {'SLA%':>6}")
    for t in top:
        off = t['offered'] or 0
        ans = t['answered'] or 0
        sla = t['ans_sla'] or 0
        pct = (sla / ans * 100) if ans > 0 else 0
        lang = t['language'] or '-'
        qn = t['queue_name'][:28]
        print(f"  {t['company']:<16} {qn:<30} {lang:<8} {off:>8,} {pct:>5.1f}%")

    print(f"\n{'='*100}")
    print("  ✅ Done")
    print(f"{'='*100}\n")

    cur.close()
    cnx.close()


if __name__ == '__main__':
    run()
