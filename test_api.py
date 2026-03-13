from flask import Flask, jsonify, render_template, request, redirect
from flask_cors import CORS
import mysql.connector
from datetime import datetime, timedelta

app = Flask(__name__, template_folder='../templates', static_folder='../static')
CORS(app)

DB_CONFIG = {
    'user': 'root', 'password': '', 'host': 'localhost',
    'database': 'dashboard_dxx',
}

#DB

def get_db():
    return mysql.connector.connect(**DB_CONFIG)

def safe_div(n, d):
    return round((n / d) * 100, 2) if d else 0.0

def fmt_sec(sec):
    sec = int(sec or 0)
    return f"{sec // 60}:{sec % 60:02d}"


#SLA / Abd formulas


def calc_sla(code, ans_in, ans_out, answered, offered, abd_in):
    if code == 'SLA2':
        return safe_div(ans_in, answered)
    if code == 'SLA3':
        denom = offered - abd_in
        return round((1 - ans_out / denom) * 100, 2) if denom > 0 else 0.0
    # SLA1 (default)
    denom = offered - abd_in
    return safe_div(ans_in, denom)

def calc_abd(code, offered, answered, abd_in, abd_out):
    if not code or code == 'NONE':
        return 0.0
    if code == 'Abd1':
        return round((1 - abd_out / offered) * 100, 2) if offered else 0.0
    if code == 'Abd2':
        return safe_div(abd_out, offered - abd_in)
    if code == 'Abd3':
        return safe_div(abd_out, answered)
    if code == 'Abd4':
        return safe_div(abd_out, offered)
    if code == 'Abd5':
        denom = offered - abd_in
        return round((1 - abd_out / denom) * 100, 2) if denom > 0 else 0.0
    return safe_div(abd_out, offered)

def abd_ok(pct, target, code):
    """Abd1/Abd5 = retention (high=good). Abd2/3/4 = loss rate (low=good)."""
    if not target:
        return True
    return pct >= target if code in ('Abd1', 'Abd5') else pct <= target



def get_meta(cursor, slug):
    cursor.execute("""
        SELECT c.id, c.name, c.slug,
               qc.sla_formula, qc.abd_formula,
               qc.tf_bh_sec, qc.tf_ooh_sec,
               qc.target_ans_pct, qc.target_abd_pct
        FROM companies c
        JOIN queue_config qc ON qc.company_id = c.id
        WHERE c.slug = %s LIMIT 1
    """, (slug,))
    r = cursor.fetchone()
    if not r:
        return None
    return {
        'id': r[0], 'name': r[1], 'slug': r[2],
        'sla': r[3] or 'SLA1', 'abd': r[4] or 'NONE',
        'tf_bh': r[5] or 30,   'tf_ooh': r[6] or 30,
        'ans_t': float(r[7] or 80), 'abd_t': float(r[8] or 5),
    }



def date_where(month, week, day, year):
    conds, params = [], []
    if day:
        conds.append("DATE(cm.interval_start) = %s"); params.append(day)
    elif week:
        try:
            yr, wn = week.split('-W')
            mon = datetime.strptime(f"{yr}-W{int(wn):02d}-1", "%Y-W%W-%w").date()
            conds.append("DATE(cm.interval_start) BETWEEN %s AND %s")
            params += [mon, mon + timedelta(days=6)]
        except Exception:
            pass
    elif month:
        conds.append("CONCAT(YEAR(cm.interval_start),'-',LPAD(MONTH(cm.interval_start),2,'0')) = %s")
        params.append(month)
    elif year:
        conds.append("YEAR(cm.interval_start) = %s"); params.append(int(year))
    return ("WHERE " + " AND ".join(conds)) if conds else "", params

def pick_table(month, week, day, year):
    return 'call_metrics_history' if any([month, week, day, year]) else 'call_metrics_today'



@app.route('/')
def root():
    return redirect('/dashboard/renault')

@app.route('/dashboard/<slug>')
def dashboard(slug):
    return render_template('dashboard_v5.html', company_slug=slug)


@app.route('/api/companies')
def list_companies():
    db = get_db(); c = db.cursor()
    try:
        c.execute("""
            SELECT c.id, c.name, c.slug, COUNT(DISTINCT qc.id) AS queue_count
            FROM companies c
            LEFT JOIN queue_config qc ON qc.company_id = c.id
            GROUP BY c.id ORDER BY c.name
        """)
        cols = [d[0] for d in c.description]
        return jsonify([dict(zip(cols, r)) for r in c.fetchall()])
    finally:
        db.close()



@app.route('/api/filters/<slug>')
def get_filters(slug):
    db = get_db(); c = db.cursor()
    try:
        meta = get_meta(c, slug)
        if not meta:
            return jsonify({'error': 'Not found'}), 404
        c.execute("""
            SELECT DISTINCT DATE(cm.interval_start) AS d,
                   YEARWEEK(cm.interval_start, 1)   AS w
            FROM call_metrics_history cm
            JOIN queue_config qc ON qc.id = cm.queue_id
            WHERE qc.company_id = %s ORDER BY d
        """, (meta['id'],))
        rows  = c.fetchall()
        days  = sorted(set(str(r[0]) for r in rows if r[0]))
        weeks = sorted(set(r[1] for r in rows if r[1]))
        return jsonify({
            'days':   days,
            'months': sorted(set(d[:7] for d in days)),
            'weeks':  [f"{str(w)[:4]}-W{str(w)[4:]}" for w in weeks],
        })
    finally:
        db.close()



@app.route('/api/today-snapshot')
def snapshot_info():
    db = get_db(); c = db.cursor()
    try:
        c.execute("SELECT DATE(interval_start), COUNT(*) FROM call_metrics_today GROUP BY 1 ORDER BY 1 DESC LIMIT 1")
        snap = c.fetchone() or (None, 0)
        c.execute("SELECT MAX(DATE(interval_start)) FROM call_metrics_history")
        latest = (c.fetchone() or [None])[0]
        return jsonify({
            'snapshot_date':  str(snap[0] or ''),
            'row_count':      snap[1],
            'history_latest': str(latest or ''),
            'is_stale':       str(snap[0]) != str(latest),
        })
    finally:
        db.close()


# (GET / PUT) 

@app.route('/api/companies/<slug>/settings', methods=['GET', 'PUT'])
def settings(slug):
    db = get_db(); c = db.cursor()
    try:
        meta = get_meta(c, slug)
        if not meta:
            return jsonify({'error': 'Not found'}), 404
        if request.method == 'GET':
            return jsonify(meta)

        d = request.json or {}
        VALID_SLA = ('SLA1','SLA2','SLA3')
        VALID_ABD = ('Abd1','Abd2','Abd3','Abd4','Abd5','NONE')
        VALID_TF  = (20, 30, 40, 45, 60, 180)
        sla = d.get('sla_code', 'SLA1'); abd = d.get('abd_code', 'NONE')
        tf_bh = int(d.get('tf_bh', 30)); tf_ooh = int(d.get('tf_ooh', 30))
        if sla not in VALID_SLA: return jsonify({'error': f'bad sla_code {sla}'}), 400
        if abd not in VALID_ABD: return jsonify({'error': f'bad abd_code {abd}'}), 400
        if tf_bh not in VALID_TF: return jsonify({'error': f'bad tf_bh {tf_bh}'}), 400

        c.execute("""
            UPDATE queue_config
            SET sla_formula=%s, abd_formula=%s,
                tf_bh_sec=%s, tf_ooh_sec=%s,
                target_ans_pct=%s, target_abd_pct=%s
            WHERE company_id=%s
        """, (sla, abd, tf_bh, tf_ooh,
              float(d.get('ans_target', 80)),
              float(d.get('abd_target', 5)),
              meta['id']))
        db.commit()
        return jsonify({'ok': True})
    finally:
        db.close()

@app.route('/api/dashboard/<slug>/summary')
@app.route('/api/dashboard/<slug>/queues')
def summary(slug):
    month  = request.args.get('month')
    week   = request.args.get('week')
    day    = request.args.get('day')
    year   = request.args.get('year')
    period = request.args.get('period', 'all')

    tbl = pick_table(month, week, day, year)
    db  = get_db(); c = db.cursor()
    try:
        meta = get_meta(c, slug)
        if not meta:
            return jsonify({'error': f'Not found: {slug}'}), 404

        where, params = date_where(month, week, day, year)
        where = (where + " AND " if where else "WHERE ") + "qc.company_id = %s"
        params.append(meta['id'])
        if period == 'bh':    where += " AND cm.is_bh = 1"
        elif period == 'ooh': where += " AND cm.is_bh = 0"

        c.execute(f"""
            SELECT
                qc.queue_name, qc.desk, qc.language,
                qc.sla_formula, qc.abd_formula,
                qc.target_ans_pct, qc.target_abd_pct,
                qc.is_exempt,
                SUM(cm.offered)       AS offered,
                SUM(cm.answered)      AS answered,
                SUM(cm.ans_in_sla)    AS ans_in,
                SUM(cm.ans_out_sla)   AS ans_out,
                SUM(cm.abd_in_sla)    AS abd_in,
                SUM(cm.abd_out_sla)   AS abd_out,
                SUM(cm.avg_handle_time * cm.answered) / NULLIF(SUM(cm.answered),0) AS aht,
                SUM(cm.avg_answer_time * cm.answered) / NULLIF(SUM(cm.answered),0) AS asa,
                SUM(cm.hold_time_total)               / NULLIF(SUM(cm.answered),0) AS hold
            FROM {tbl} cm
            JOIN queue_config qc ON qc.id = cm.queue_id
            {where}
            GROUP BY qc.id, qc.queue_name, qc.desk, qc.language,
                     qc.sla_formula, qc.abd_formula,
                     qc.target_ans_pct, qc.target_abd_pct, qc.is_exempt
            ORDER BY qc.queue_name
        """, params)
        rows = c.fetchall()
    finally:
        db.close()

    queue_list = []
    tot = dict(offered=0, answered=0, ans_in=0, ans_out=0, abd_in=0, abd_out=0,
               aht_w=0, asa_w=0, hold_w=0)

    for r in rows:
        offered  = int(r[8]  or 0); answered = int(r[9]  or 0)
        ans_in   = int(r[10] or 0); ans_out  = int(r[11] or 0)
        abd_in   = int(r[12] or 0); abd_out  = int(r[13] or 0)
        aht      = float(r[14] or 0); asa = float(r[15] or 0); hold = float(r[16] or 0)

        q_sla  = r[3] or meta['sla']; q_abd = r[4] or meta['abd']
        q_ans_t = float(r[5] or meta['ans_t']); q_abd_t = float(r[6] or meta['abd_t'])
        exempt  = bool(r[7])

        sla_pct = calc_sla(q_sla, ans_in, ans_out, answered, offered, abd_in)
        abd_pct = calc_abd(q_abd, offered, answered, abd_in, abd_out)

        if exempt or offered == 0:
            status = 'OK'
        else:
            status = 'OK' if (sla_pct >= q_ans_t and abd_ok(abd_pct, q_abd_t, q_abd)) else 'Breach'

        queue_list.append({
            'name': r[0], 'desk': r[1], 'language': r[2],
            'offered': offered, 'answered': answered, 'abandoned': offered - answered,
            'ans_in_sla': ans_in, 'abd_in_sla': abd_in, 'abd_out_sla': abd_out,
            'sla_pct': sla_pct, 'abd_pct': abd_pct, 'hdl_pct': safe_div(answered, offered),
            'aht_fmt': fmt_sec(aht), 'asa_fmt': fmt_sec(asa), 'hold_fmt': fmt_sec(hold),
            'status': status, 'exempt': exempt,
        })

        tot['offered']  += offered;  tot['answered'] += answered
        tot['ans_in']   += ans_in;   tot['ans_out']  += ans_out
        tot['abd_in']   += abd_in;   tot['abd_out']  += abd_out
        tot['aht_w']    += aht  * answered
        tot['asa_w']    += asa  * answered
        tot['hold_w']   += hold * answered

    t_off = tot['offered']; t_ans = tot['answered']
    t_sla = calc_sla(meta['sla'], tot['ans_in'], tot['ans_out'], t_ans, t_off, tot['abd_in'])
    t_abd = calc_abd(meta['abd'], t_off, t_ans, tot['abd_in'], tot['abd_out'])

    return jsonify({
        'company': meta['name'], 'slug': meta['slug'],
        'sla_code': meta['sla'], 'abd_code': meta['abd'],
        'ans_target': meta['ans_t'], 'abd_target': meta['abd_t'],
        'total_offered': t_off, 'total_answered': t_ans, 'total_abandoned': t_off - t_ans,
        'total_ans_in_sla': tot['ans_in'], 'total_abd_in_sla': tot['abd_in'],
        'sla_pct': t_sla, 'abd_pct': t_abd, 'hdl_pct': safe_div(t_ans, t_off),
        'aht_fmt':  fmt_sec(tot['aht_w']  / t_ans if t_ans else 0),
        'asa_fmt':  fmt_sec(tot['asa_w']  / t_ans if t_ans else 0),
        'hold_fmt': fmt_sec(tot['hold_w'] / t_ans if t_ans else 0),
        'total_queues':   len(queue_list),
        'breached_count': sum(1 for q in queue_list if q['status'] == 'Breach'),
        'languages': sorted(list(set(q['language'] for q in queue_list if q['language']))),
        'queues': queue_list,
    })


@app.route('/api/dashboard/<slug>/intraday')
def intraday(slug):
    month  = request.args.get('month')
    week   = request.args.get('week')
    day    = request.args.get('day')
    year   = request.args.get('year')
    queue  = request.args.get('queue', 'ALL')
    period = request.args.get('period', 'all')

    tbl = pick_table(month, week, day, year)
    db  = get_db(); c = db.cursor()
    try:
        meta = get_meta(c, slug)
        if not meta:
            return jsonify({'error': 'Not found'}), 404

        where, params = date_where(month, week, day, year)
        where = (where + " AND " if where else "WHERE ") + "qc.company_id = %s"
        params.append(meta['id'])
        if queue != 'ALL':
            where += " AND qc.queue_name = %s"; params.append(queue)
        if period == 'bh':    where += " AND cm.is_bh = 1"
        elif period == 'ooh': where += " AND cm.is_bh = 0"

        c.execute(f"""
            SELECT
                CONCAT(LPAD(HOUR(cm.interval_start),2,'0'),':',
                       LPAD(MINUTE(cm.interval_start),2,'0')) AS slot,
                SUM(cm.offered)     AS offered,
                SUM(cm.answered)    AS answered,
                SUM(cm.ans_in_sla)  AS ans_in,
                SUM(cm.ans_out_sla) AS ans_out,
                SUM(cm.abd_in_sla)  AS abd_in,
                SUM(cm.abd_out_sla) AS abd_out
            FROM {tbl} cm
            JOIN queue_config qc ON qc.id = cm.queue_id
            {where}
            GROUP BY slot ORDER BY slot
        """, params)
        rows = c.fetchall()
    finally:
        db.close()

    result = []
    for r in rows:
        off = int(r[1] or 0); ans = int(r[2] or 0)
        result.append({
            'slot': r[0], 'offered': off, 'answered': ans,
            'sla_pct': calc_sla(meta['sla'], int(r[3] or 0), int(r[4] or 0), ans, off, int(r[5] or 0)),
            'abd_pct': calc_abd(meta['abd'], off, ans, int(r[5] or 0), int(r[6] or 0)),
        })
    return jsonify(result)

if __name__ == '__main__':
    print("DXC Dashboard → http://127.0.0.1:5000/")
    app.run(debug=True, port=5000, use_reloader=False)