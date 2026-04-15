# render_engine.py — HTML rendering engine for SPX Quant Engine
# Each function returns a complete HTML string for st.components.v1.html()

import json
import hashlib


def _uid():
    return hashlib.md5(str(id(object())).encode()).hexdigest()[:8]


def _base_css():
    return """<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#080b12;--card:#0c0f18;--card2:#0e1220;--border:rgba(0,200,232,.1);
--border2:rgba(0,200,232,.25);--accent:#00c8e8;--pos:#00e676;--neg:#ff4d4d;
--warn:#f5a623;--text:#d8dce8;--muted:rgba(255,255,255,.28);--dim:rgba(255,255,255,.12);
--mono:'JetBrains Mono','Fira Code','Cascadia Code',monospace;
--sans:-apple-system,'Segoe UI',system-ui,sans-serif;--r:6px}
body{background:var(--bg);color:var(--text);font-family:var(--sans);font-size:13px;line-height:1.5}
.block-title{font-size:9px;text-transform:uppercase;letter-spacing:1.2px;color:var(--accent);
opacity:.55;border-left:2px solid rgba(0,200,232,.3);padding-left:7px;margin-bottom:9px}
.tag{display:inline-flex;align-items:center;padding:3px 8px;border-radius:4px;font-size:11px;font-weight:500;font-family:var(--sans)}
.tag-pos{background:rgba(0,230,118,.1);color:rgba(0,230,118,.8);border:1px solid rgba(0,230,118,.2)}
.tag-neg{background:rgba(255,77,77,.1);color:rgba(255,120,120,.8);border:1px solid rgba(255,77,77,.2)}
.tag-acc{background:rgba(0,200,232,.08);color:var(--accent);border:1px solid rgba(0,200,232,.2)}
.tag-neutral{background:rgba(255,255,255,.05);color:var(--muted);border:1px solid rgba(255,255,255,.1)}
.metrics-row{display:grid;gap:8px;margin-bottom:10px}
.metric-card{background:var(--card);border:1px solid var(--border);border-radius:var(--r);padding:8px 10px;text-align:center}
.metric-card.pos{border-color:rgba(0,230,118,.2)}.metric-card.neg{border-color:rgba(255,77,77,.2)}.metric-card.acc{border-color:rgba(0,200,232,.2)}
.cw{position:relative;border:1px solid var(--border);border-radius:var(--r);background:var(--card);cursor:pointer;overflow:hidden;transition:transform .15s,box-shadow .15s;margin:8px 0}
.cw:hover{transform:scale(1.015);box-shadow:0 2px 16px rgba(0,200,232,.1)}
.cw canvas{display:block;width:100%!important}
.cw-hint{position:absolute;bottom:6px;right:8px;font-size:9px;color:var(--dim);font-family:var(--mono);opacity:0;transition:opacity .15s;pointer-events:none}
.cw:hover .cw-hint{opacity:1}
.modal-ov{display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.8);z-index:9999;align-items:center;justify-content:center}
.modal-ov.open{display:flex}
.modal-box{background:#0e1220;border:1px solid rgba(0,200,232,.2);border-radius:10px;width:min(820px,94vw);max-height:84vh;display:flex;flex-direction:column}
.modal-hdr{display:flex;align-items:center;justify-content:space-between;padding:10px 14px;border-bottom:1px solid var(--border)}
.modal-ttl{font-size:12px;font-weight:600;color:var(--text)}
.mbtn{background:transparent;border:1px solid var(--border);border-radius:4px;color:var(--muted);font-size:10px;padding:3px 8px;cursor:pointer;font-family:var(--mono)}
.mbtn:hover{border-color:var(--accent);color:var(--accent)}
.mclose{width:22px;height:22px;border-radius:50%;background:rgba(255,77,77,.1);border:1px solid rgba(255,77,77,.2);color:rgba(255,110,110,.8);cursor:pointer;display:flex;align-items:center;justify-content:center;font-size:13px}
.mclose:hover{background:rgba(255,77,77,.2)}
.modal-body{padding:14px;flex:1;overflow:auto}
.modal-body canvas{max-height:480px!important}
.metric-label{font-size:11px;color:#a0a8b8;text-transform:uppercase;letter-spacing:.8px;margin-bottom:4px}
.metric-value{font-family:var(--mono);font-size:26px;font-weight:700;color:var(--text)}
.metric-sub{font-size:10px;color:var(--muted);margin-top:2px}
.v-pos{color:var(--pos)}.v-neg{color:var(--neg)}.v-acc{color:var(--accent)}.v-neu{color:var(--text)}
.data-table{width:100%;border-collapse:collapse;font-family:var(--mono);font-size:11px}
.data-table th{font-size:14px;font-weight:500;text-transform:uppercase;letter-spacing:.8px;color:#a0a8b8;padding:8px 12px;border-bottom:1px solid rgba(255,255,255,.08);font-family:var(--sans);text-align:left;white-space:nowrap}
.data-table td{font-size:17px;padding:8px 12px;border-bottom:1px solid rgba(255,255,255,.04);color:var(--text)}
.data-table tr:hover td{background:rgba(255,255,255,.02)}
.conclusion{background:rgba(0,200,232,.08);border:1px solid rgba(0,200,232,.25);
border-left:3px solid #00c8e8;border-radius:6px;padding:12px 16px;
font-size:13px;color:#d8dce8;margin-bottom:12px;line-height:1.5}
.conclusion .arrow{color:#00c8e8;font-family:var(--mono)}
.chart-wrapper{position:relative;border:1px solid var(--border);border-radius:var(--r);overflow:hidden;background:var(--card);margin:8px 0}
.chart-wrapper canvas{display:block;width:100%!important}
.header-row{display:flex;align-items:center;gap:8px;margin-bottom:10px;flex-wrap:wrap}
.header-title{font-size:16px;font-weight:700;color:var(--text);font-family:var(--sans)}
.header-meta{font-size:10px;color:var(--dim);margin-left:auto}
.card-row{display:flex;gap:6px;margin:6px 0;flex-wrap:wrap}
.engulfing-card{flex:1;min-width:200px;background:var(--card);border-radius:var(--r);padding:10px 14px;display:flex;justify-content:space-between;align-items:center;gap:8px}
.engulfing-card.ok{border-left:3px solid var(--pos)}.engulfing-card.fail{border-left:3px solid var(--neg)}
</style>"""


def _chart_js():
    return """<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/chartjs-plugin-annotation/3.0.1/chartjs-plugin-annotation.min.js"></script>
<script>
window._QC=window._QC||{};
function _openModal(id,title){var o=document.getElementById('mov-'+id);if(!o)return;o.classList.add('open');var t=o.querySelector('.modal-ttl');if(t)t.textContent=title;setTimeout(function(){if(window._QC['m'+id])window._QC['m'+id].resize()},60)}
function _closeModal(id){var o=document.getElementById('mov-'+id);if(o)o.classList.remove('open')}
function _exportPNG(id){var c=window._QC['m'+id]||window._QC[id];if(!c)return;var a=document.createElement('a');a.download='spxq-'+id+'.png';a.href=c.toBase64Image('image/png',1);a.click()}
document.addEventListener('keydown',function(e){if(e.key==='Escape')document.querySelectorAll('.modal-ov.open').forEach(function(m){m.classList.remove('open')})});
var _CD={responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'rgba(255,255,255,.35)',font:{size:10}}},tooltip:{backgroundColor:'#0e1220',borderColor:'rgba(0,200,232,.25)',borderWidth:1,titleColor:'rgba(255,255,255,.65)',bodyColor:'rgba(255,255,255,.45)',padding:8,cornerRadius:4}},scales:{x:{grid:{color:'rgba(255,255,255,.04)'},ticks:{color:'rgba(255,255,255,.28)',font:{size:10}}},y:{grid:{color:'rgba(255,255,255,.04)'},ticks:{color:'rgba(255,255,255,.28)',font:{size:10}}}}};
</script>"""


def _metric(label, value, sub="", sentiment="neutral"):
    vc = {"positive": "v-pos", "negative": "v-neg", "accent": "v-acc"}.get(sentiment, "v-neu")
    cc = {"positive": "pos", "negative": "neg", "accent": "acc"}.get(sentiment, "")
    return (f'<div class="metric-card {cc}"><div class="metric-label">{label}</div>'
            f'<div class="metric-value {vc}">{value}</div>'
            + (f'<div class="metric-sub">{sub}</div>' if sub else "") + '</div>')


def _metrics_row(cards, cols=4):
    html = f'<div class="metrics-row" style="grid-template-columns:repeat({cols},1fr);">'
    for c in cards:
        html += _metric(c.get("label", ""), c.get("value", ""), c.get("sub", ""), c.get("s", "neutral"))
    return html + '</div>'


def _header(title, tags=None, meta=""):
    t = ''.join(f'<span class="tag {tg[1]}">{tg[0]}</span>' for tg in (tags or []))
    return (f'<div class="header-row"><span class="header-title">{title}</span>{t}'
            f'<span class="header-meta">{meta}</span></div>')


def _conclusion(text):
    if not text:
        return ""
    return f'<div class="conclusion"><span class="arrow">→</span> {text}</div>'


def _table(headers, rows):
    h = ''.join(f'<th>{h}</th>' for h in headers)
    r = ''.join('<tr>' + ''.join(f'<td>{c}</td>' for c in row) + '</tr>' for row in rows)
    return f'<table class="data-table"><thead><tr>{h}</tr></thead><tbody>{r}</tbody></table>'


# ─── Renderers ───────────────────────────────────────────


def render_engulfing(result):
    uid = _uid()
    ticker = result.get("ticker", "")
    pattern = result.get("pattern", "bearish engulfing")
    n = result.get("n_total", 0)
    taux = result.get("taux", 0)
    ns = result.get("n_success", 0)
    nf = result.get("n_fail", 0)
    seuil = result.get("seuil", 2.0)
    rows = result.get("rows", [])
    conc = result.get("conclusion", "")

    pattern_display = pattern.replace("bearish engulfing", "bearish E").replace("bullish engulfing", "bullish E")
    tag_type = "tag-neg" if "bearish" in pattern.lower() else "tag-pos"
    taux_s = "positive" if taux >= 60 else "negative"

    html = _base_css() + _chart_js()
    html += _header(f"{ticker} — {pattern_display}",
                    [(f"seuil {seuil}%", "tag-acc"), (f"{n} occ.", "tag-neutral")],
                    f"taux {taux:.1f}%")
    html += _metrics_row([
        {"label": "Occurrences", "value": str(n), "s": "neutral"},
        {"label": "Taux succès", "value": f"{taux:.1f}%", "s": taux_s},
        {"label": "Succès", "value": str(ns), "s": "positive"},
        {"label": "Échecs", "value": str(nf), "s": "negative"},
    ])
    html += _conclusion(conc)

    # Cards for N <= 5, table for N > 5
    if rows and len(rows) <= 5:
        for d in rows:
            vj = d.get("var_j", d.get("var", 0)) or 0
            vj1 = d.get("next_var", 0) or 0
            ok = d.get("success", False)
            cls = "ok" if ok else "fail"
            cj = "v-pos" if vj > 0 else "v-neg"
            cj1 = "v-pos" if vj1 > 0 else "v-neg"
            html += (f'<div class="engulfing-card {cls}">'
                     f'<span style="font-size:12px;font-weight:600;color:var(--text);min-width:80px">{d.get("date","")}</span>'
                     f'<span class="{cj}" style="font-family:var(--mono);font-size:13px;font-weight:700">{vj:+.2f}%</span>'
                     f'<span style="font-family:var(--mono);font-size:12px;color:var(--muted)">{d.get("close",0):.2f}</span>'
                     f'<span class="{cj1}" style="font-family:var(--mono);font-size:13px;font-weight:700">{vj1:+.2f}%</span>'
                     f'<span class="tag {"tag-pos" if ok else "tag-neg"}">{"Succès" if ok else "Échec"}</span></div>')
    elif rows:
        headers = ["Date", "Var J %", "Close", "Résultat", "Amp. max %"]
        trows = []
        for d in rows:
            res = '<span class="v-pos">Succès</span>' if d.get("success") else '<span class="v-neg">Échec</span>'
            vj = d.get("var_j", d.get("var", 0)) or 0
            vc = "v-pos" if vj > 0 else "v-neg"
            trows.append([d.get("date", ""), f'<span class="{vc}">{vj:+.2f}</span>',
                          f'{d.get("close", 0):.2f}', res, f'{d.get("best_move", 0):.2f}'])
        html += _table(headers, trows)

    # Scatter chart with modal
    if rows and len(rows) >= 3 and all("best_move" in d for d in rows):
        ok_pts = json.dumps([{"x": round(d.get("var_j", d.get("var", 0)) or 0, 2),
                               "y": round(d.get("best_move", 0), 2),
                               "date": d.get("date", "")}
                              for d in rows if d.get("success")])
        fail_pts = json.dumps([{"x": round(d.get("var_j", d.get("var", 0)) or 0, 2),
                                 "y": round(d.get("best_move", 0), 2),
                                 "date": d.get("date", "")}
                                for d in rows if not d.get("success")])
        title_sc = f"{ticker} — Scatter Var J vs Amplitude"
        html += f'''<div class="cw" onclick="_openModal('{uid}','{title_sc}')" style="padding:8px">
        <canvas id="c-{uid}" height="228"></canvas>
        <span class="cw-hint">clic pour agrandir</span></div>
        <div class="modal-ov" id="mov-{uid}"><div class="modal-box">
          <div class="modal-hdr"><span class="modal-ttl">{title_sc}</span>
            <div style="display:flex;gap:6px"><button class="mbtn" onclick="_exportPNG('{uid}')">↓ PNG</button>
            <div class="mclose" onclick="_closeModal('{uid}')">×</div></div></div>
          <div class="modal-body"><canvas id="cm-{uid}" style="height:420px"></canvas></div>
        </div></div>
        <script>(function(){{
        var data={{datasets:[
          {{label:'Succès',data:{ok_pts},backgroundColor:'rgba(0,230,118,.7)',pointRadius:5,pointHoverRadius:8}},
          {{label:'Échec',data:{fail_pts},backgroundColor:'rgba(255,77,77,.7)',pointRadius:5,pointHoverRadius:8}}
        ]}};
        var opts={{responsive:true,maintainAspectRatio:false,
          plugins:{{legend:{{labels:{{color:'rgba(255,255,255,.35)',font:{{size:10}}}}}},
            tooltip:{{backgroundColor:'#0e1220',borderColor:'rgba(0,200,232,.25)',borderWidth:1,
              titleColor:'rgba(255,255,255,.65)',bodyColor:'rgba(255,255,255,.45)',
              callbacks:{{label:function(c){{var d=c.raw;return[d.date||'','Var J: '+(d.x>0?'+':'')+d.x.toFixed(2)+'%','Amp: '+d.y.toFixed(2)+'%'].filter(Boolean)}}}}}}}},
          scales:{{
            x:{{title:{{display:true,text:'Var J (%)',color:'#a0a8b8',font:{{size:11}}}},grid:{{color:'rgba(255,255,255,.04)'}},ticks:{{color:'rgba(255,255,255,.3)'}}}},
            y:{{title:{{display:true,text:'Amp. max J+5 (%)',color:'#a0a8b8',font:{{size:11}}}},grid:{{color:'rgba(255,255,255,.04)'}},ticks:{{color:'rgba(255,255,255,.3)'}}}}
          }}
        }};
        var m=document.getElementById('c-{uid}');if(m)window._QC['{uid}']=new Chart(m,{{type:'scatter',data:JSON.parse(JSON.stringify(data)),options:opts}});
        var mm=document.getElementById('cm-{uid}');if(mm)window._QC['m{uid}']=new Chart(mm,{{type:'scatter',data:JSON.parse(JSON.stringify(data)),options:opts}});
        }})();</script>'''

    return html


def render_engulfing_by_year(result):
    uid = _uid()
    ticker = result.get("ticker", "")
    pattern = result.get("pattern", "")
    pattern_display = pattern.replace("bearish engulfing", "bearish E").replace("bullish engulfing", "bullish E")
    seuil = result.get("seuil", 2.0)
    yr = result.get("year_rows", [])
    tot = result.get("total", {})
    conc = result.get("conclusion", "")

    html = _base_css() + _chart_js()
    html += _header(f"{ticker} — {pattern_display} par année", [(f"seuil {seuil}%", "tag-acc")])
    html += _conclusion(conc)

    if yr:
        headers = ["Année", "Occ.", "Succès", "Échecs", "Taux %"]
        trows = [[str(r["Année"]), str(r["Occurrences"]), str(r["Succès"]), str(r["Échecs"]),
                   f'<span class="{"v-pos" if r["Taux %"] >= 60 else "v-neg"}">{r["Taux %"]:.1f}%</span>']
                  for r in yr]
        html += _table(headers, trows)

        labels = json.dumps([str(r["Année"]) for r in yr])
        vals = json.dumps([r["Taux %"] for r in yr])
        colors = json.dumps(["rgba(0,230,118,.7)" if r["Taux %"] >= 60 else "rgba(255,77,77,.7)" for r in yr])
        html += f'''<div class="cw" style="padding:8px">
        <canvas id="by-{uid}" height="160"></canvas>
        <span class="cw-hint">clic pour agrandir</span></div>
        <script>(function(){{
        var canvas=document.getElementById('by-{uid}');
        var chart=new Chart(canvas, {{
          type:'bar',
          data:{{labels:{labels},datasets:[{{data:{vals},backgroundColor:{colors},borderRadius:3}}]}},
          options:{{responsive:true,maintainAspectRatio:false,
            plugins:{{legend:{{display:false}}}},
            scales:{{y:{{grid:{{color:'rgba(255,255,255,.04)'}},ticks:{{color:'rgba(255,255,255,.3)'}}}},
                     x:{{grid:{{display:false}},ticks:{{color:'rgba(255,255,255,.3)'}}}}}}}}
        }});
        canvas.addEventListener('click',function(e){{if(e.detail>1||document.getElementById('spxq-modal'))return;var modal=document.createElement('div');modal.id='spxq-modal';modal.style.cssText='position:fixed;top:0;left:0;width:100vw;height:100vh;background:rgba(8,11,18,.96);z-index:9999;display:flex;flex-direction:column;align-items:center;justify-content:center;';var cb=document.createElement('button');cb.innerHTML='✕';cb.style.cssText='position:absolute;top:16px;right:20px;background:transparent;border:1px solid #444;color:#ccc;font-size:18px;width:36px;height:36px;border-radius:50%;cursor:pointer;';var bc=document.createElement('canvas');bc.style.cssText='width:90vw;max-width:1200px;max-height:75vh;';var dl=document.createElement('button');dl.innerHTML='↓ PNG';dl.style.cssText='margin-top:12px;background:#0d1a0d;border:1px solid #26a269;color:#26a269;padding:6px 16px;border-radius:6px;font-size:13px;cursor:pointer;';modal.appendChild(cb);modal.appendChild(bc);modal.appendChild(dl);document.body.appendChild(modal);var bc2;try{{bc2=new Chart(bc,JSON.parse(JSON.stringify(chart.config)));bc2.update()}}catch(err){{}}var close=function(){{if(bc2)bc2.destroy();if(document.getElementById('spxq-modal'))document.body.removeChild(modal)}};cb.addEventListener('click',close);var esc=function(ev){{if(ev.key==='Escape'){{close();document.removeEventListener('keydown',esc)}}}};document.addEventListener('keydown',esc);dl.addEventListener('click',function(){{var a=document.createElement('a');a.download='spxq_'+Date.now()+'.png';a.href=bc.toDataURL('image/png');a.click()}});}});
        }})();</script>'''

    if tot:
        html += _metrics_row([
            {"label": "Total", "value": str(tot.get("n", 0)), "s": "neutral"},
            {"label": "Succès", "value": str(tot.get("n_success", 0)), "sub": f'{tot.get("taux", 0):.1f}%', "s": "positive"},
            {"label": "Échecs", "value": str(tot.get("n_fail", 0)), "s": "negative"},
        ], cols=3)

    # Détail par année
    dates_detail = result.get("dates_detail", [])
    all_years = [r["Année"] for r in yr]

    if dates_detail:
        for year in all_years:
            yr_dates = [d for d in dates_detail if d.get("date", "")[-4:] == str(year)]
            if not yr_dates:
                continue
            ns_yr = sum(1 for d in yr_dates if d.get("success"))
            nf_yr = len(yr_dates) - ns_yr
            html += f'<div style="margin-top:16px;border-left:3px solid rgba(0,200,232,.3);padding-left:12px">'
            html += f'<div style="font-size:15px;font-weight:600;color:#a0a8b8;margin-bottom:8px">{year} — {len(yr_dates)} occurrences · {ns_yr} succès · {nf_yr} échec{"s" if nf_yr > 1 else ""}</div>'
            trows = []
            for d in yr_dates:
                vj = d.get("var_j", d.get("var", 0)) or 0
                vc = "v-pos" if vj > 0 else "v-neg"
                res = '<span class="v-pos">Succès</span>' if d.get("success") else '<span class="v-neg">Échec</span>'
                amp = d.get("best_move", 0)
                trows.append([
                    d.get("date", ""),
                    f'<span class="{vc}">{vj:+.2f}%</span>',
                    f'{d.get("close", 0):.2f}',
                    res,
                    f'{amp:.2f}%'
                ])
            html += _table(["Date", "Var J", "Close", "Résultat", "Amp. max"], trows)
            html += '</div>'

    # Scatter toutes périodes
    if dates_detail and len(dates_detail) >= 3 and all("best_move" in d for d in dates_detail):
        uid_sc = _uid()
        ok_pts = json.dumps([{"x": round(d.get("var_j", d.get("var", 0)) or 0, 2),
                               "y": round(d.get("best_move", 0), 2),
                               "date": d.get("date", ""),
                               "year": d.get("date", "")[-4:]}
                              for d in dates_detail if d.get("success")])
        fail_pts = json.dumps([{"x": round(d.get("var_j", d.get("var", 0)) or 0, 2),
                                 "y": round(d.get("best_move", 0), 2),
                                 "date": d.get("date", ""),
                                 "year": d.get("date", "")[-4:]}
                                for d in dates_detail if not d.get("success")])
        title_sc = f"{ticker} — Scatter Var J vs Amplitude (toutes périodes)"
        html += f'''<div class="cw" onclick="_openModal('{uid_sc}','{title_sc}')" style="padding:8px;margin-top:16px">
        <canvas id="c-{uid_sc}" height="228"></canvas>
        <span class="cw-hint">clic pour agrandir</span></div>
        <div class="modal-ov" id="mov-{uid_sc}"><div class="modal-box">
          <div class="modal-hdr"><span class="modal-ttl">{title_sc}</span>
            <div style="display:flex;gap:6px">
              <button class="mbtn" onclick="_exportPNG('{uid_sc}')">↓ PNG</button>
              <div class="mclose" onclick="_closeModal('{uid_sc}')">×</div>
            </div>
          </div>
          <div class="modal-body"><canvas id="cm-{uid_sc}" style="height:420px"></canvas></div>
        </div></div>
        <script>(function(){{
        var data={{datasets:[
          {{label:'Succès',data:{ok_pts},backgroundColor:'rgba(0,230,118,.7)',
            pointRadius:5,pointHoverRadius:8}},
          {{label:'Échec',data:{fail_pts},backgroundColor:'rgba(255,77,77,.7)',
            pointRadius:5,pointHoverRadius:8}}
        ]}};
        var opts={{responsive:true,maintainAspectRatio:false,
          plugins:{{legend:{{labels:{{color:'rgba(255,255,255,.35)',font:{{size:10}}}}}},
            tooltip:{{backgroundColor:'#0e1220',borderColor:'rgba(0,200,232,.25)',borderWidth:1,
              titleColor:'rgba(255,255,255,.65)',bodyColor:'rgba(255,255,255,.45)',
              callbacks:{{label:function(c){{
                var d=c.raw;
                return[d.date+' ('+d.year+')','Var J: '+(d.x>0?'+':'')+d.x.toFixed(2)+'%','Amp: '+d.y.toFixed(2)+'%'];
              }}}}}}}},
          scales:{{
            x:{{title:{{display:true,text:'Var J (%)',color:'#a0a8b8',font:{{size:11}}}},
               grid:{{color:'rgba(255,255,255,.04)'}},ticks:{{color:'rgba(255,255,255,.3)'}}}},
            y:{{title:{{display:true,text:'Amp. max J+5 (%)',color:'#a0a8b8',font:{{size:11}}}},
               grid:{{color:'rgba(255,255,255,.04)'}},ticks:{{color:'rgba(255,255,255,.3)'}}}}
          }}
        }};
        var m=document.getElementById('c-{uid_sc}');
        if(m)window._QC['{uid_sc}']=new Chart(m,{{type:'scatter',data:JSON.parse(JSON.stringify(data)),options:opts}});
        var mm=document.getElementById('cm-{uid_sc}');
        if(mm)window._QC['m{uid_sc}']=new Chart(mm,{{type:'scatter',data:JSON.parse(JSON.stringify(data)),options:opts}});
        }})();</script>'''

    return html


def render_best_single(result):
    sub = result.get("sub_type", "")
    html = _base_css()

    if sub in ("single_value", "single_value_enriched"):
        val = result.get("value", 0)
        unit = result.get("unit", "")
        label = result.get("label", "")
        sign = "+" if val > 0 and unit == "%" else ""
        s = "positive" if val > 0 and unit == "%" else ("negative" if val < 0 and unit == "%" else "accent")
        html += _header(label)
        html += _metrics_row([{"label": label.split("—")[-1].strip() if "—" in label else "Valeur",
                                "value": f"{sign}{val:.2f} {unit}", "s": s}], cols=1)
        ctx = result.get("context", {})
        if ctx:
            cards = []
            if ctx.get("var_pct") is not None:
                v = ctx["var_pct"]
                cards.append({"label": "Var J", "value": f"{v:+.2f}%", "s": "positive" if v > 0 else "negative"})
            if ctx.get("volume_ratio"):
                cards.append({"label": "Volume", "value": f"{ctx['volume_ratio']:.1f}x moy", "s": "neutral"})
            if ctx.get("vix"):
                cards.append({"label": "VIX", "value": f"{ctx['vix']:.1f}", "s": "neutral"})
            if cards:
                html += _metrics_row(cards, cols=len(cards))
            if ctx.get("pattern"):
                html += f'<div style="font-size:10px;color:var(--accent);margin-top:4px">Pattern : {ctx["pattern"]}</div>'

    elif sub == "best_single":
        ticker = result.get("ticker", "")
        label = result.get("label", "")
        date = result.get("date", "")
        var = result.get("var", 0)
        close = result.get("close", 0)
        s = "positive" if var > 0 else "negative"
        html += _header(label, [(date, "tag-acc")])
        html += _metrics_row([
            {"label": "Variation", "value": f"{'+' if var > 0 else ''}{var:.2f}%", "s": s},
            {"label": "Date", "value": date, "s": "neutral"},
            {"label": "Clôture", "value": f"{close:,.2f}", "s": "neutral"},
        ], cols=3)

    elif sub == "best_multi":
        ticker = result.get("ticker", "")
        label = result.get("label", "")
        results_list = result.get("results", [])
        html += _header(f"{ticker} — {label}", meta=f"{len(results_list)} année(s)")
        cards = []
        for r in results_list:
            p = r.get("var", 0)
            cards.append({"label": str(r.get("year", "")),
                          "value": f"{'+' if p > 0 else ''}{p:.2f}%",
                          "sub": r.get("date", ""), "s": "positive" if p > 0 else "negative"})
        html += _metrics_row(cards, cols=min(len(cards), 4))

    elif sub == "pattern_last":
        ticker = result.get("ticker", "")
        pattern = result.get("pattern", "")
        pattern_display = pattern.replace("bearish engulfing", "bearish E").replace("bullish engulfing", "bullish E")
        date = result.get("date", "")
        var = result.get("var", 0)
        close = result.get("close", 0)
        nv = result.get("next_var")
        n_total = result.get("n_total", 0)
        s = "negative" if var < 0 else "positive"
        html += _header(f"{ticker} — Dernier {pattern_display}", [(date, "tag-acc"), (f"{n_total} occ. total", "tag-neutral")])
        cards = [
            {"label": "Date", "value": date, "s": "neutral"},
            {"label": "Variation J", "value": f"{'+' if var > 0 else ''}{var:.2f}%", "s": s},
            {"label": "Clôture", "value": f"{close:,.2f}", "s": "neutral"},
        ]
        if nv is not None:
            cards.append({"label": "Var J+1", "value": f"{'+' if nv > 0 else ''}{nv:.2f}%",
                          "s": "positive" if nv > 0 else "negative"})
        html += _metrics_row(cards, cols=len(cards))

    elif sub in ("count", "engulfing_avg_perf"):
        if sub == "count":
            html += _metrics_row([
                {"label": result.get("label", "Jours"), "value": str(result.get("count", 0)), "s": "accent"},
                {"label": "% du total", "value": f"{result.get('pct', 0):.1f}%", "sub": f"{result.get('total', 0)} séances", "s": "neutral"},
            ], cols=2)
        else:
            html += _header(f"{result.get('ticker','')} — Perf moy après {result.get('pattern','')}")
            html += _metrics_row([
                {"label": "Var moy J+1", "value": f"{result.get('avg_next', 0):+.2f}%", "s": "positive" if (result.get('avg_next', 0) or 0) > 0 else "negative"},
                {"label": "Var méd J+1", "value": f"{result.get('med_next', 0):+.2f}%", "s": "neutral"},
                {"label": "% positif", "value": f"{result.get('pct_pos_next', 0):.1f}%", "s": "neutral"},
            ], cols=3)
            html += _conclusion(result.get("conclusion", ""))

    return html


def render_annual_perf(result):
    uid = _uid()
    sub = result.get("sub_type", "")
    html = _base_css() + _chart_js()

    if sub == "annual_multi":
        ticker = result.get("ticker", "")
        rl = result.get("results", [])
        html += _header(f"{ticker} — Performance annuelle", meta=f"{len(rl)} années")
        cards = [{"label": str(r["year"]), "value": f"{'+' if r['perf'] > 0 else ''}{r['perf']:.2f}%",
                  "sub": f"{r.get('first_close', 0):.0f}→{r.get('last_close', 0):.0f}",
                  "s": "positive" if r["perf"] > 0 else "negative"} for r in rl]
        html += _metrics_row(cards, cols=min(len(cards), 4))

        labels = json.dumps([str(r["year"]) for r in rl])
        vals = json.dumps([r["perf"] for r in rl])
        colors = json.dumps(["rgba(0,230,118,.7)" if r["perf"] > 0 else "rgba(255,77,77,.7)" for r in rl])
        html += f'''<div class="cw" style="padding:8px">
        <canvas id="ap-{uid}" height="160"></canvas>
        <span class="cw-hint">clic pour agrandir</span></div>
        <script>(function(){{
        var canvas=document.getElementById('ap-{uid}');
        var chart=new Chart(canvas,{{type:'bar',data:{{labels:{labels},datasets:[{{data:{vals},backgroundColor:{colors},borderRadius:3}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:{{y:{{grid:{{color:'rgba(255,255,255,.04)'}},ticks:{{color:'rgba(255,255,255,.3)'}}}},x:{{grid:{{display:false}},ticks:{{color:'rgba(255,255,255,.3)'}}}}}}}}
        }});
        canvas.addEventListener('click',function(e){{if(e.detail>1||document.getElementById('spxq-modal'))return;var modal=document.createElement('div');modal.id='spxq-modal';modal.style.cssText='position:fixed;top:0;left:0;width:100vw;height:100vh;background:rgba(8,11,18,.96);z-index:9999;display:flex;flex-direction:column;align-items:center;justify-content:center;';var cb=document.createElement('button');cb.innerHTML='✕';cb.style.cssText='position:absolute;top:16px;right:20px;background:transparent;border:1px solid #444;color:#ccc;font-size:18px;width:36px;height:36px;border-radius:50%;cursor:pointer;';var bc=document.createElement('canvas');bc.style.cssText='width:90vw;max-width:1200px;max-height:75vh;';var dl=document.createElement('button');dl.innerHTML='↓ PNG';dl.style.cssText='margin-top:12px;background:#0d1a0d;border:1px solid #26a269;color:#26a269;padding:6px 16px;border-radius:6px;font-size:13px;cursor:pointer;';modal.appendChild(cb);modal.appendChild(bc);modal.appendChild(dl);document.body.appendChild(modal);var bc2;try{{bc2=new Chart(bc,JSON.parse(JSON.stringify(chart.config)));bc2.update()}}catch(err){{}}var close=function(){{if(bc2)bc2.destroy();if(document.getElementById('spxq-modal'))document.body.removeChild(modal)}};cb.addEventListener('click',close);document.addEventListener('keydown',function esc(ev){{if(ev.key==='Escape'){{close();document.removeEventListener('keydown',esc)}}}});dl.addEventListener('click',function(){{var a=document.createElement('a');a.download='spxq_'+Date.now()+'.png';a.href=bc.toDataURL('image/png');a.click()}});}});
        }})();</script>'''
    return html


def render_bias(result):
    html = _base_css()
    ticker = result.get("ticker", "")
    biais = result.get("biais", "neutre")
    sm = {"haussier": "positive", "baissier": "negative"}.get(biais, "neutral")
    html += _header(f"{ticker} — Biais", [(biais.upper(), f"tag-{'pos' if biais == 'haussier' else 'neg' if biais == 'baissier' else 'neutral'}")])
    html += _metrics_row([
        {"label": "% jours positifs", "value": f"{result.get('pct_pos', 50):.1f}%", "s": sm},
        {"label": "Var. moyenne", "value": f"{result.get('mean_var', 0):+.3f}%", "s": "positive" if result.get("mean_var", 0) > 0 else "negative"},
        {"label": "Var. médiane", "value": f"{result.get('median_var', 0):+.3f}%", "s": "neutral"},
        {"label": "Skewness", "value": f"{result.get('skew', 0):.2f}", "s": "neutral"},
    ])
    spx = result.get("spx_context", {})
    if spx:
        html += f'<div style="font-size:10px;color:var(--muted);margin-top:6px">SPX : {spx.get("pct_pos", 0):.1f}% positif · moy {spx.get("mean_var", 0):+.3f}%</div>'
    return html


def render_neutral_next(result):
    ticker = result.get("ticker", "")
    rank = result.get("rank", 1)
    rank_label = {1: "1er", 2: "2ème", 3: "3ème"}.get(rank, f"{rank}ème")
    date = result.get("date", "")
    vj = result.get("var_J", 0)
    vj1 = result.get("var_J1", 0)
    close = result.get("close")
    thr = result.get("threshold", 5.0)
    n_total = result.get("n_total", 0)
    top5 = result.get("top5", [])

    j1c = "var(--pos)" if vj1 > 0 else "var(--neg)" if vj1 < 0 else "var(--accent)"
    j1s = "+" if vj1 > 0 else ""

    html = _base_css()
    html += _header(f"{ticker} — Var J+1 la plus neutre",
                    [(f"{rank_label} rang", "tag-neutral"), (f"seuil -{thr}%", "tag-neutral")],
                    f"{n_total} occurrences")
    html += _metrics_row([
        {"label": "Date", "value": date, "s": "accent"},
        {"label": "Var J", "value": f"{vj:+.2f}%", "s": "negative"},
        {"label": "Var J+1", "value": f"{j1s}{vj1:.2f}%", "sub": f"≈ 0% ({abs(vj1):.2f}% d'écart)", "s": "accent"},
    ] + ([{"label": "Close", "value": f"{close:.2f}", "s": "neutral"}] if close else []),
                          cols=4 if close else 3)

    if top5:
        html += '<div class="block-title" style="margin-top:10px">Top 5 plus neutres J+1</div>'
        rows_html = []
        for r in top5:
            is_cur = r["rank"] == rank
            bg = "rgba(0,200,232,0.06)" if is_cur else ""
            v1c = "v-pos" if r["var_J1"] > 0 else "v-neg" if r["var_J1"] < 0 else "v-acc"
            v1s = "+" if r["var_J1"] > 0 else ""
            rows_html.append([
                f'<span style="color:{"var(--accent)" if is_cur else "var(--muted)"}">#{r["rank"]}</span>',
                r["date"],
                f'<span class="v-neg">{r["var_J"]:+.2f}%</span>',
                f'<span class="{v1c}">{v1s}{r["var_J1"]:.2f}%</span>',
                f'{r["abs_J1"]:.2f}%',
            ])
        html += _table(["Rang", "Date", "Var J", "Var J+1", "|Var J+1|"], rows_html)

    html += _conclusion(result.get("conclusion", ""))
    return html


def render_correlation(result):
    html = _base_css()
    c = result.get("corr", 0)
    interp = "forte +" if c > .6 else "modérée +" if c > .3 else "faible" if c > -.3 else "modérée −" if c > -.6 else "forte −"
    s = "positive" if c > .3 else ("negative" if c < -.3 else "neutral")
    html += _header(f"Corrélation {result.get('ticker', '')} / {result.get('ticker_2', '')}", meta=f"{result.get('n', 0)} séances")
    html += _metrics_row([
        {"label": "Pearson", "value": f"{c:+.4f}", "s": s},
        {"label": "Interprétation", "value": interp, "s": s},
    ], cols=2)
    return html


def render_correlation_scan(result):
    html = _base_css()
    ticker = result.get("ticker", "")
    rows = result.get("results", [])
    html += _header(f"{ticker} — Corrélations", meta=f"{result.get('n_assets', 0)} actifs")
    if rows:
        headers = ["Actif", "Corrélation", "Force", "N jours"]
        trows = []
        for r in rows:
            v = r["Corrélation"]
            vc = "v-pos" if v > 0 else "v-neg"
            trows.append([r["Actif"], f'<span class="{vc}">{v:+.4f}</span>', r["Force"], str(r["N jours"])])
        html += _table(headers, trows)
    html += _conclusion(result.get("conclusion", ""))
    return html


def render_streak(result):
    html = _base_css()
    ticker = result.get("ticker", "")
    d = result.get("direction", "up")
    label = "haussière" if d == "up" else "baissière"
    best = result.get("best", {})
    avg = result.get("avg_streak", 0)
    html += _header(f"{ticker} — Séquences {label}")
    if best:
        html += _metrics_row([
            {"label": "Record", "value": f"{best.get('length', 0)} j", "s": "positive" if d == "up" else "negative"},
            {"label": "Du", "value": best.get("start", ""), "s": "neutral"},
            {"label": "Au", "value": best.get("end", ""), "s": "neutral"},
            {"label": "Moy", "value": f"{avg:.1f} j", "s": "neutral"},
        ])
    top5 = result.get("top5", [])
    if top5:
        html += _table(["Durée", "Début", "Fin"], [[str(t["length"]), t["start"], t.get("end", "")] for t in top5])
    html += _conclusion(result.get("conclusion", ""))
    return html


def render_multi_threshold(result):
    uid = _uid()
    html = _base_css() + _chart_js()
    ticker = result.get("ticker", "")
    rows = result.get("results", [])
    html += _header(f"{ticker} — Multi-seuils")
    if rows:
        headers = list(rows[0].keys())
        trows = [[str(r.get(h, "")) for h in headers] for r in rows]
        html += _table(headers, trows)
        has_j1 = any(r.get("% positif J+1") is not None for r in rows)
        y_key = "% positif J+1" if has_j1 else "Occurrences"
        labels = json.dumps([r["Seuil"] for r in rows])
        vals = json.dumps([r.get(y_key, 0) or 0 for r in rows])
        html += f'''<div class="cw" style="padding:8px">
        <canvas id="mt-{uid}" height="160"></canvas>
        <span class="cw-hint">clic pour agrandir</span></div>
        <script>(function(){{
        var canvas=document.getElementById('mt-{uid}');
        var chart=new Chart(canvas,{{type:'bar',data:{{labels:{labels},datasets:[{{label:'{y_key}',data:{vals},backgroundColor:'rgba(0,200,232,.6)',borderRadius:3}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:{{y:{{grid:{{color:'rgba(255,255,255,.04)'}},ticks:{{color:'rgba(255,255,255,.3)'}}}},x:{{grid:{{display:false}},ticks:{{color:'rgba(255,255,255,.3)'}}}}}}}}
        }});
        canvas.addEventListener('click',function(e){{if(e.detail>1||document.getElementById('spxq-modal'))return;var modal=document.createElement('div');modal.id='spxq-modal';modal.style.cssText='position:fixed;top:0;left:0;width:100vw;height:100vh;background:rgba(8,11,18,.96);z-index:9999;display:flex;flex-direction:column;align-items:center;justify-content:center;';var cb=document.createElement('button');cb.innerHTML='✕';cb.style.cssText='position:absolute;top:16px;right:20px;background:transparent;border:1px solid #444;color:#ccc;font-size:18px;width:36px;height:36px;border-radius:50%;cursor:pointer;';var bc=document.createElement('canvas');bc.style.cssText='width:90vw;max-width:1200px;max-height:75vh;';var dl=document.createElement('button');dl.innerHTML='↓ PNG';dl.style.cssText='margin-top:12px;background:#0d1a0d;border:1px solid #26a269;color:#26a269;padding:6px 16px;border-radius:6px;font-size:13px;cursor:pointer;';modal.appendChild(cb);modal.appendChild(bc);modal.appendChild(dl);document.body.appendChild(modal);var bc2;try{{bc2=new Chart(bc,JSON.parse(JSON.stringify(chart.config)));bc2.update()}}catch(err){{}}var close=function(){{if(bc2)bc2.destroy();if(document.getElementById('spxq-modal'))document.body.removeChild(modal)}};cb.addEventListener('click',close);document.addEventListener('keydown',function esc(ev){{if(ev.key==='Escape'){{close();document.removeEventListener('keydown',esc)}}}});dl.addEventListener('click',function(){{var a=document.createElement('a');a.download='spxq_'+Date.now()+'.png';a.href=bc.toDataURL('image/png');a.click()}});}});
        }})();</script>'''
    html += _conclusion(result.get("conclusion", ""))
    return html


def render_ml_amplitude(result):
    html = _base_css()
    pred = result.get("prediction", {})
    stats = result.get("model_stats", {})
    entry = result.get("entry_point", "9h30")
    cat = pred.get("amplitude_category", "?")
    amp = pred.get("amplitude_pct", 0)
    pts = pred.get("amplitude_pts", 0)
    probas = pred.get("probabilities", {})

    cat_colors = {"FORT": "pos", "FAIBLE": "acc", "INCERTAIN": "neutral"}
    cc = cat_colors.get(cat, "neutral")
    html += _header("SPX — Prédiction ML", [(entry, "tag-acc"), (cat, f"tag-{cc}")])
    html += _metrics_row([
        {"label": "Signal", "value": cat, "s": {"FORT": "positive", "FAIBLE": "accent"}.get(cat, "neutral")},
        {"label": "Amplitude", "value": f"{amp:.2f}%", "sub": f"~{pts:.0f} pts", "s": "neutral"},
        {"label": "Précision", "value": f"{stats.get('category_accuracy', 0):.1f}%", "s": "accent"},
        {"label": "MAE", "value": f"{stats.get('amplitude_mae', 0):.4f}%", "s": "neutral"},
    ])
    if probas:
        html += (f'<div style="font-size:10px;color:var(--dim);margin:6px 0">'
                 f'FORT:{probas.get("fort",0):.0f}% · INCERTAIN:{probas.get("incertain",0):.0f}% · FAIBLE:{probas.get("faible",0):.0f}%</div>')

    ric = pred.get("ric_signal", False)
    ic = pred.get("ic_signal", False)
    if ric:
        html += f'<div style="color:var(--pos);font-size:11px;margin:4px 0">✅ Signal RIC — amplitude ≥ 0.45% depuis {entry}</div>'
    elif ic:
        html += f'<div style="color:var(--accent);font-size:11px;margin:4px 0">✅ Signal IC — amplitude ≤ 0.23% depuis {entry}</div>'

    top_f = result.get("top_features", {})
    if top_f:
        items = list(top_f.items())[:8]
        html += '<div style="margin-top:8px"><div class="block-title">Features prédictives</div>'
        for k, v in items:
            pct = min(v / max(top_f.values()) * 100, 100)
            html += (f'<div style="display:flex;align-items:center;gap:6px;margin:2px 0;font-size:10px">'
                     f'<span style="width:140px;color:var(--muted);font-family:var(--mono);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{k}</span>'
                     f'<div style="flex:1;height:4px;background:var(--card);border-radius:2px;overflow:hidden">'
                     f'<div style="width:{pct:.0f}%;height:100%;background:rgba(145,65,172,.6);border-radius:2px"></div></div>'
                     f'<span style="color:var(--dim);font-family:var(--mono);width:40px;text-align:right">{v:.4f}</span></div>')
        html += '</div>'

    html += _conclusion(result.get("conclusion", ""))
    return html


def render_intraday_amplitude(result):
    html = _base_css()
    entry = result.get("entry_point", "9h30")
    stats = result.get("stats", {})
    n = stats.get("n_sessions", 0)
    cond = result.get("condition", "")
    html += _header(f"SPX/SPY — {'Amplitude' if not cond else cond}", [(entry, "tag-acc")], f"{n} sessions")
    rows = stats.get("rows", [])
    if rows:
        headers = list(rows[0].keys())
        trows = [[str(r.get(h, "")) for h in headers] for r in rows]
        html += _table(headers, trows)
    html += _conclusion(result.get("conclusion", ""))
    return html


def render_intraday_overnight(result):
    html = _base_css()
    html += _header("SPX — Overnight futures", meta=f"{result.get('n', 0)} jours")
    html += _metrics_row([
        {"label": "Corrél. dir.", "value": f"{result.get('corr_dir_pct', result.get('pct_same_direction', 0)):.1f}%", "s": "neutral"},
        {"label": "Ret overnight moy", "value": f"{result.get('mean_overnight_ret', 0):+.3f}%", "s": "neutral"},
    ], cols=2)
    html += _conclusion(result.get("conclusion", ""))
    return html


def render_text_explanation(result):
    html = _base_css()
    subject = result.get("subject", result.get("pattern", ""))
    html += _header(f"{subject} — Explication")
    text = result.get("text", "")
    lignes = result.get("lignes", text.split("\n") if text else [])
    for line in lignes:
        if line.strip():
            html += f'<div style="font-size:15px;color:#d8dce8;margin:6px 0;padding:6px 12px;border-left:2px solid rgba(0,200,232,.4);line-height:1.6">{line}</div>'
    return html


def render_engulfing_failure(result):
    html = _base_css()
    ticker = result.get("ticker", "")
    nf = result.get("n_failures", 0)
    html += _header(f"{ticker} — Échecs engulfing", [(f"{nf} échecs", "tag-neg")])
    corr = result.get("correlations", {})
    if corr:
        headers = ["Métrique", "Valeur"]
        trows = [[k, str(v)] for k, v in corr.items()]
        html += _table(headers, trows)
    html += _conclusion(result.get("conclusion", ""))
    return html


def render_engulfing_vol(result):
    html = _base_css()
    ticker = result.get("ticker", "")
    target = result.get("target_rate", 70)
    opt = result.get("optimal")
    html += _header(f"{ticker} — Volume minimum (BE)", [(f"cible {target}%", "tag-acc")])
    if opt:
        html += _metrics_row([
            {"label": "Vol ratio min", "value": f"{opt['Vol ratio min']:.2f}x", "s": "accent"},
            {"label": "Taux succès", "value": f"{opt['Taux succès %']:.1f}%", "s": "positive"},
            {"label": "Occurrences", "value": str(opt["N occurrences"]), "s": "neutral"},
        ], cols=3)
    rows = result.get("results", [])
    if rows:
        html += _table(["Vol ratio min", "N occ.", "Taux %"],
                        [[str(r["Vol ratio min"]), str(r["N occurrences"]), f'{r["Taux succès %"]:.1f}'] for r in rows])
    html += _conclusion(result.get("conclusion", ""))
    return html


def render_engulfing_duration(result):
    html = _base_css()
    html += _header(f"{result.get('ticker','')} — Durée baisse après {result.get('pattern','')}")
    html += _metrics_row([
        {"label": "Médiane", "value": f"{result.get('median_days', 0)} j", "s": "neutral"},
        {"label": "Moyenne", "value": f"{result.get('mean_days', 0):.1f} j", "s": "neutral"},
        {"label": "N", "value": str(result.get("n", 0)), "s": "neutral"},
    ], cols=3)
    html += _conclusion(result.get("conclusion", ""))
    return html


def render_multi_condition(result):
    html = _base_css()
    a1 = result.get("asset_1", "")
    a2 = result.get("asset_2", "")
    c1 = result.get("cond_1", "")
    c2 = result.get("cond_2", "")
    n = result.get("n", 0)
    html += _header(f"{a2} quand {a1}", [(f"{n} jours", "tag-acc")])
    if n > 0:
        html += _metrics_row([
            {"label": "Occurrences", "value": str(n), "s": "neutral"},
            {"label": "% positif J+1", "value": f"{result.get('pct_pos_next', 0):.1f}%", "s": "positive" if result.get("pct_pos_next", 0) > 50 else "negative"},
            {"label": "Var moy J+1", "value": f"{result.get('mean_next', 0):+.2f}%", "s": "neutral"},
        ], cols=3)
    return html


def render_filter_abs(result):
    html = _base_css()
    ticker = result.get("ticker", "")
    n = result.get("n", 0)
    thr = result.get("threshold", 0)
    variation_type = result.get("variation_type", "close-to-close")
    html += _header(f"{ticker} — Jours ≥ {thr}% ({variation_type})", [(f"{n} jours", "tag-acc")])
    html += _metrics_row([
        {"label": "Nb jours", "value": str(n), "s": "neutral"},
        {"label": "% positif J+1", "value": f"{result.get('pct_positive_next', 0):.1f}%", "s": "neutral"},
        {"label": "Var moy J+1", "value": f"{result.get('mean_next', 0):+.2f}%", "s": "neutral"},
    ], cols=3)
    return html


def render_spx_overnight(result):
    html = _base_css()
    active = result.get("active_patterns", [])
    all_p = result.get("all_patterns", [])
    html += _header("SPX Overnight", [(f"{len(active)} actifs", "tag-pos" if active else "tag-neutral")])
    if active:
        for p in active:
            tag = "tag-pos" if p.get("actionnable") else "tag-neutral"
            html += (f'<div style="background:var(--card);border:1px solid var(--border);border-radius:var(--r);'
                     f'padding:8px 12px;margin:4px 0;display:flex;justify-content:space-between;align-items:center">'
                     f'<span class="tag {tag}">{"ACT" if p.get("actionnable") else "OBS"}</span>'
                     f'<span style="font-size:12px;color:var(--text)">{p["label"]}</span>'
                     f'<span style="font-family:var(--mono);font-size:13px;color:var(--accent)">{p["taux_is"]:.1f}%</span>'
                     f'<span style="font-size:10px;color:var(--dim)">n={p["n"]}</span></div>')
    else:
        html += '<div style="font-size:11px;color:var(--muted);padding:8px">Aucun pattern actif aujourd\'hui.</div>'
    if all_p:
        html += '<div class="block-title" style="margin-top:10px">Top patterns historiques</div>'
        headers = ["Signal", "Dir.", "IS %", "OOS %", "N"]
        trows = [[p["label"], p["direction"], f'{p["taux_is"]:.1f}',
                   f'{p.get("taux_oos", "—")}' if p.get("taux_oos") else "—", str(p["n"])]
                  for p in all_p[:10]]
        html += _table(headers, trows)
    return html


def render_engulfing_multi_period(result):
    uid = _uid()
    ticker = result.get("ticker", "")
    pattern = result.get("pattern", "bearish engulfing")
    pattern_display = pattern.replace("bearish engulfing", "bearish E").replace("bullish engulfing", "bullish E")
    prs = result.get("period_results", [])
    html = _base_css() + _chart_js()
    html += _header(f"{ticker} — {pattern_display} — Multi-périodes",
                    [{"text": f"{len(prs)} années", "type": "acc"}])
    for pr in prs:
        year = pr.get("year", "?")
        html += f'<div style="border-left:3px solid rgba(0,200,232,.3);padding-left:12px;margin:12px 0 6px">'
        html += f'<div style="font-size:13px;font-weight:600;color:#a0a8b8;margin-bottom:6px">{year}</div>'
        html += _metrics_row([
            {"label": "Occ.", "value": str(pr.get("n_total", 0)), "s": "neutral"},
            {"label": "Taux", "value": f"{pr.get('taux', 0):.1f}%",
             "s": "positive" if pr.get("taux", 0) >= 60 else "negative"},
            {"label": "Succès", "value": str(pr.get("n_success", 0)), "s": "positive"},
            {"label": "Échecs", "value": str(pr.get("n_fail", 0)), "s": "negative"},
        ])
        rows = pr.get("rows", [])
        if rows and len(rows) <= 5:
            for d in rows:
                vj = d.get("var_j", 0) or 0
                vj1 = d.get("next_var", 0) or 0
                ok = d.get("success", False)
                html += (f'<div class="engulfing-card {"ok" if ok else "fail"}">'
                         f'<span style="font-size:12px;font-weight:600;color:var(--text);min-width:80px">{d.get("date","")}</span>'
                         f'<span class="{"v-pos" if vj>0 else "v-neg"}" style="font-family:var(--mono);font-size:13px;font-weight:700">{vj:+.2f}%</span>'
                         f'<span style="font-family:var(--mono);font-size:12px;color:var(--muted)">{d.get("close",0):.2f}</span>'
                         f'<span class="{"v-pos" if vj1>0 else "v-neg"}" style="font-family:var(--mono);font-size:13px">{vj1:+.2f}%</span>'
                         f'<span class="tag {"tag-pos" if ok else "tag-neg"}">{"S" if ok else "E"}</span></div>')
        elif rows:
            trows = [[d.get("date",""),
                       f'<span class="{"v-pos" if (d.get("var_j",0) or 0)>0 else "v-neg"}">{(d.get("var_j",0) or 0):+.2f}</span>',
                       f'{d.get("close",0):.2f}',
                       '<span class="v-pos">S</span>' if d.get("success") else '<span class="v-neg">E</span>',
                       f'{d.get("best_move",0):.2f}']
                      for d in rows]
            html += _table(["Date","Var J","Close","Rés.","Amp."], trows)
        html += '</div>'
    if len(prs) >= 2:
        html += '<div style="border-top:1px solid rgba(255,255,255,.08);margin-top:16px;padding-top:12px">'
        html += '<div style="font-size:13px;font-weight:600;color:#00c8e8;margin-bottom:8px">Synthèse</div>'
        srows = []
        for pr in prs:
            vc = "v-pos" if pr.get("taux",0)>=60 else "v-neg"
            srows.append([str(pr.get("year","")), str(pr.get("n_total",0)),
                           f'<span class="{vc}">{pr.get("taux",0):.1f}%</span>'])
        html += _table(["Année","N","Taux %"], srows)
        html += '</div>'
    return html


def render_fallback(result, error=""):
    html = _base_css()
    html += f'<div style="color:var(--warn);font-size:12px;margin:8px 0">⚠ {error or "Rendu non disponible"}</div>'
    for k in ("ticker", "n", "conclusion", "label", "value"):
        if k in result:
            html += f'<div style="font-size:11px;color:var(--muted);margin:2px 0"><b>{k}</b>: {result[k]}</div>'
    return html


# ─── Dispatch ────────────────────────────────────────────

def dispatch_render(result):
    sub = result.get("sub_type", "")
    DISPATCH = {
        "engulfing_multi_period": (render_engulfing_multi_period, 600),
        "engulfing_analysis": (render_engulfing, 480),
        "engulfing_by_year": (render_engulfing_by_year, 500),
        "engulfing_failure_analysis": (render_engulfing_failure, 420),
        "best_single": (render_best_single, 220),
        "best_multi": (render_best_single, 220),
        "single_value": (render_best_single, 220),
        "single_value_enriched": (render_best_single, 240),
        "annual_multi": (render_annual_perf, 440),
        "bias_analysis": (render_bias, 220),
        "correlation": (render_correlation, 220),
        "correlation_scan": (render_correlation_scan, 500),
        "streak_analysis": (render_streak, 380),
        "multi_threshold": (render_multi_threshold, 460),
        "multi_condition": (render_multi_condition, 220),
        "ml_amplitude": (render_ml_amplitude, 420),
        "ml_prediction": (render_ml_amplitude, 420),
        "intraday_amplitude": (render_intraday_amplitude, 320),
        "intraday_conditional": (render_intraday_amplitude, 320),
        "intraday_overnight": (render_intraday_overnight, 220),
        "intraday_best_time": (render_intraday_amplitude, 320),
        "intraday_general": (render_intraday_amplitude, 260),
        "text_explanation_general": (render_text_explanation, 320),
        "text_explanation": (render_text_explanation, 320),
        "spx_overnight": (render_spx_overnight, 520),
        "engulfing_volume_threshold": (render_engulfing_vol, 280),
        "engulfing_avg_perf": (render_best_single, 220),
        "engulfing_duration": (render_engulfing_duration, 220),
        "engulfing_thresholds": (render_multi_threshold, 320),
        "engulfing_vix": (render_correlation_scan, 320),
        "filter_abs": (render_filter_abs, 220),
        "neutral_next": (render_neutral_next, 440),
        "count": (render_best_single, 220),
        "weekday": (render_intraday_amplitude, 460),
        "monthly": (render_intraday_amplitude, 460),
        "pattern_last": (render_best_single, 240),
        "pattern_all": (render_correlation_scan, 520),
    }
    if sub in DISPATCH:
        fn, h = DISPATCH[sub]
        # Dynamic height based on content
        if sub == "engulfing_multi_period":
            n_years = len(result.get("period_results", []))
            n_rows = sum(r.get("n_total", 0) for r in result.get("period_results", []))
            h = 180 * n_years + min(n_rows * 22, 300) + 320
            h = max(h, 500)
        elif sub == "engulfing_by_year":
            year_rows = result.get("year_rows", [])
            dates_detail = result.get("dates_detail", [])
            n_years = len(year_rows)
            n_total_rows = len(dates_detail)
            has_scatter = len(dates_detail) >= 3 and any("best_move" in d for d in dates_detail)
            h = 200 + n_years * 38 + 300
            h += n_years * 50 + n_total_rows * 38
            h += 280 if has_scatter else 0
            h = max(h, 500)
        elif sub in ("engulfing_analysis", "engulfing_thresholds"):
            n = result.get("n_total", result.get("n", 0)) or 0
            rows = result.get("rows", [])
            has_chart = n >= 3 and any("best_move" in d for d in rows)
            h = (130 + n * 52 + (300 if has_chart else 0)) if n <= 5 else (300 + min(n * 22, 240) + (300 if has_chart else 0))
            h = max(h, 200)
        elif sub in ("best_single", "single_value", "count", "engulfing_avg_perf"):
            h = 220
        elif sub == "best_multi":
            h = max(220, 140 + min(len(result.get("results", [])) * 30, 180))
        elif sub == "correlation_scan":
            h = max(260, 150 + min(len(result.get("results", [])) * 28, 320))
        elif sub == "annual_multi":
            h = max(280, 190 + min(len(result.get("results", [])) * 30, 220))
        elif sub in ("filter_abs", "multi_condition"):
            h = max(260, 180 + min(len(result.get("dates", [])) * 22, 320))
        try:
            return fn(result), h
        except Exception as e:
            return render_fallback(result, str(e)), 200
    return render_fallback(result, f"sub_type '{sub}' non géré"), 200
