import re, pandas as pd, mysql.connector
from datetime import timedelta

PATH_CONFIG  = r"raw_data\Copy of SLA 2(Sheet1).csv"
PATH_METRICS = r"raw_data\DB all queues(Historical Metrics Report).csv"

DB_CONFIG = {
    'user': 'root', 'password': '', 'host': 'localhost',
    'database': 'dashboard_dxx',
}

def slugify(s):
    return re.sub(r'[^a-z0-9]+', '-', s.lower().strip()).strip('-')

def fval(row, col, default=0):
    v = row.get(col, default)
    if pd.isna(v): return default
    try: return float(str(v).replace(',','').strip())
    except: return default

def ival(row, col, default=0):
    return int(fval(row, col, default))

def is_bh(dt):
    return 1 if (dt.weekday() < 5 and 8 <= dt.hour < 18) else 0

def parse_pct(v):
    """Parse '90%' or '5%' → float, or None."""
    if pd.isna(v): return None
    try: return float(str(v).replace('%','').replace('sec','').strip())
    except: return None
LANG_MAP = {
    'French': 'French', 'FR': 'French',
    'English': 'English', 'Eng': 'English', 'UK': 'English', 'EN': 'English',
    'German': 'German', 'Ger': 'German', 'DE': 'German',
    'Spanish': 'Spanish', 'SP': 'Spanish', 'ES': 'Spanish',
    'Italian': 'Italian', 'ITA': 'Italian',
    'Portuguese': 'Portuguese', 'PT': 'Portuguese', 'Brazil': 'Portuguese',
    'Dutch': 'Dutch', 'NL': 'Dutch', 'DU': 'Dutch',
    'Turkish': 'Turkish', 'TR': 'Turkish',
    'Arabic': 'Arabic', 'ARA': 'Arabic',
    'Hungarian': 'Hungarian', 'HU': 'Hungarian',
    'Polish': 'Polish', 'Pol': 'Polish',
    'Nordic': 'Nordic', 'Swedish': 'Nordic', 'Norwegian': 'Nordic', 'Danish': 'Nordic', 'Finnish': 'Nordic'
}

def detect_language(desk: str, queue_name: str) -> str | None:
    text = f" {desk} {queue_name} "
    sorted_codes = sorted(LANG_MAP.keys(), key=len, reverse=True)
    for code in sorted_codes:
        if re.search(rf'(?<![a-zA-Z]){re.escape(code)}(?![a-zA-Z])', text, re.I):
            return LANG_MAP[code]
    return None

ANS_COLS = {
    20:'Contacts answered in 20 seconds',  30:'Contacts answered in 30 seconds',
    40:'Contacts answered 40 seconds',     45:'Contacts answered in 45 seconds',
    60:'Contacts answered in 60 seconds', 180:'Contacts answered in 180 seconds',
}
ABD_COLS = {
    20:'Contacts abandoned in 20 seconds',  30:'Contacts abandoned in 30 seconds',
    40:'Contacts abandoned 40 seconds',     45:'Contacts abandoned in 45 seconds',
    60:'Contacts abandoned in 60 seconds', 180:'Contacts abandoned in 180 seconds',
}

def best_col(mapping, seconds, available):
    if seconds in mapping and mapping[seconds] in available:
        return mapping[seconds]
    for t in sorted(mapping):
        if t >= seconds and mapping[t] in available:
            return mapping[t]
    return None

def run():
    print("=" * 50)
    print("ELT → dashboard_dxx")
    print("=" * 50)

    # 1) Load CSVs
    print("\n[1/4] Loading CSVs…")
    try:
        df_cfg = pd.read_csv(PATH_CONFIG,  encoding='latin1')
        df_met = pd.read_csv(PATH_METRICS, encoding='latin1')
    except FileNotFoundError as e:
        print(f"❌ {e}"); return

    df_cfg.columns = [c.strip() for c in df_cfg.columns]
    df_met.columns = [c.strip() for c in df_met.columns]
    df_cfg = df_cfg.drop_duplicates(subset='Queue name', keep='first')
    print(f"  Config: {len(df_cfg):,} rows | Metrics: {len(df_met):,} rows")

    # read SLA/abd rules directly from CSV
    print("\n[2/4] Parsing queue config…")
    config_rows = {}   
    company_rules = {} 

    for _, row in df_cfg.iterrows():
        qname = str(row.get('Queue name','')).strip()
        if not qname or qname.lower() == 'nan': continue

        comp = str(row.get('account','')).strip()
        desk = str(row.get('Desk','')).strip()
        try:    tf_bh  = int(float(str(row.get('Timeframe BH', 30)).strip()))
        except: tf_bh  = 30
        try:    tf_ooh = int(float(str(row.get('Timeframe OOH',30)).strip()))
        except: tf_ooh = 30

        # Read SLA/abd formulas + targets straight from the CSV
        sla = str(row.get('SLA answered','SLA1')).strip()
        abd = str(row.get('abd rate','')).strip()
        if not sla or sla.lower() == 'nan': sla = 'SLA1'
        if not abd or abd.lower() == 'nan': abd = 'NONE'

        ans_t = parse_pct(row.get('Target Ans rate'))
        abd_t = parse_pct(row.get('Target Abd rate'))
        lang  = detect_language(desk, qname)

        config_rows[qname] = {
            'company': comp, 'desk': desk, 'language': lang,
            'tf_bh': tf_bh, 'tf_ooh': tf_ooh,
            'sla': sla, 'abd': abd,
            'ans_t': ans_t, 'abd_t': abd_t,
        }

        # Keep first-seen company rules for metric threshold lookup
        if comp and comp not in company_rules:
            company_rules[comp] = {'tf_bh': tf_bh, 'tf_ooh': tf_ooh}

    print(f"  {len(config_rows):,} unique queues from {len(company_rules)} companies")

    # 3) Parse metrics
    print("\n[3/4] Parsing metrics…")
    avail = set(df_met.columns)
    if 'StartInterval' in avail:
        df_met['_dt'] = pd.to_datetime(df_met['StartInterval'], errors='coerce')
    else:
        df_met['_dt'] = pd.to_datetime(
            df_met.get('StartDate','').astype(str)+' '+df_met.get('StartTime','').astype(str),
            errors='coerce')
    df_met = df_met.dropna(subset=['_dt'])
    df_met['_bh'] = df_met['_dt'].apply(is_bh)
    print(f"  Matched {df_met['Queue'].isin(config_rows).sum():,} / {len(df_met):,} rows")

    # 4) Write to MySQL
    print("\n[4/4] Writing to MySQL…")
    try:
        cnx = mysql.connector.connect(**DB_CONFIG)
        cur = cnx.cursor()
    except mysql.connector.Error as e:
        print(f"❌ {e}"); return

    # Companies
    company_ids = {}
    for comp in sorted(set(v['company'] for v in config_rows.values() if v['company'])):
        sl = slugify(comp)
        cur.execute("INSERT IGNORE INTO companies (name,slug) VALUES (%s,%s)", (comp,sl))
        cur.execute("SELECT id FROM companies WHERE slug=%s", (sl,))
        r = cur.fetchone()
        if r: company_ids[comp] = r[0]
    cnx.commit()
    print(f"  ✔ {len(company_ids)} companies")

    # queue_config — all rules come from config_rows (read from CSV)
    queue_ids = {}
    for qname, cfg in config_rows.items():
        cid = company_ids.get(cfg['company'])
        if not cid: continue
        cur.execute("""
            INSERT INTO queue_config
              (company_id,queue_name,desk,language,sla_formula,abd_formula,
               tf_bh_sec,tf_ooh_sec,target_ans_pct,target_abd_pct,is_exempt)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              desk=VALUES(desk), language=VALUES(language),
              sla_formula=VALUES(sla_formula),
              abd_formula=VALUES(abd_formula), tf_bh_sec=VALUES(tf_bh_sec),
              tf_ooh_sec=VALUES(tf_ooh_sec), target_ans_pct=VALUES(target_ans_pct),
              target_abd_pct=VALUES(target_abd_pct), is_exempt=VALUES(is_exempt)
        """, (cid, qname, cfg['desk'] or None, cfg['language'],
              cfg['sla'], cfg['abd'],
              cfg['tf_bh'], cfg['tf_ooh'],
              cfg['ans_t'], cfg['abd_t'], 0))
        cur.execute("SELECT id FROM queue_config WHERE company_id=%s AND queue_name=%s",(cid,qname))
        row = cur.fetchone()
        if row: queue_ids[qname] = row[0]
    cnx.commit()
    print(f"  ✔ {len(queue_ids)} queues")

    # call_metrics_history
    BATCH = 500
    written = 0
    batch = []
    for _, row in df_met.iterrows():
        qname = str(row.get('Queue','')).strip()
        qid = queue_ids.get(qname)
        if not qid: continue

        cfg = config_rows[qname]
        dt  = row['_dt']
        bh  = int(row['_bh'])
        tf  = cfg['tf_bh'] if bh else cfg['tf_ooh']

        offered   = ival(row, 'Contacts queued')
        answered  = ival(row, 'Contacts handled incoming')
        abandoned = max(0, offered - answered)

        # Buckets
        a = {s: min(ival(row, ANS_COLS[s]) if ANS_COLS[s] in avail else 0, answered)
             for s in (20,30,40,45,60,180)}
        d = {s: min(ival(row, ABD_COLS[s]) if ABD_COLS[s] in avail else 0, abandoned)
             for s in (20,30,40,45,60,180)}

        # SLA pre-compute
        ac = best_col(ANS_COLS, tf, avail)
        dc = best_col(ABD_COLS, tf, avail)
        ans_in = min(ival(row, ac) if ac else 0, answered)
        abd_in = min(ival(row, dc) if dc else 0, abandoned)

        batch.append((
            qid,
            dt.strftime('%Y-%m-%d %H:%M:%S'),
            (dt + timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S'),
            bh, offered, answered, abandoned,
            a[20],a[30],a[40],a[45],a[60],a[180],
            d[20],d[30],d[40],d[45],d[60],d[180],
            ans_in, max(0, answered - ans_in),
            abd_in, max(0, abandoned - abd_in),
            fval(row,'Average handle time') or None,
            fval(row,'Average queue answer time') or None,
            int(fval(row,'Customer hold time')) if fval(row,'Customer hold time') else None,
        ))

        if len(batch) >= BATCH:
            _insert(cur, batch); written += len(batch); batch = []

    if batch:
        _insert(cur, batch); written += len(batch)

    cnx.commit(); cur.close(); cnx.close()
    print(f"  ✔ {written:,} metrics rows written")
    print(f"\n✅ ELT complete → dashboard_dxx\n")


def _insert(cur, batch):
    cur.executemany("""
        INSERT IGNORE INTO call_metrics_history (
          queue_id, interval_start, interval_end, is_bh,
          offered, answered, abandoned,
          ans_in_20,ans_in_30,ans_in_40,ans_in_45,ans_in_60,ans_in_180,
          abd_in_20,abd_in_30,abd_in_40,abd_in_45,abd_in_60,abd_in_180,
          ans_in_sla, ans_out_sla, abd_in_sla, abd_out_sla,
          avg_handle_time, avg_answer_time, hold_time_total
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, batch)


if __name__ == '__main__':
    run()
