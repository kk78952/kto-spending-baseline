/**
 * KTO Daily Spending Baseline Dashboard — redesigned wide layout
 */

const { useState, useEffect, useMemo, useRef } = React;

// ── Constants ─────────────────────────────────────────────────────────────────
const COHORTS = [
  { id: "whale",   label: "Whale",   vip: "VIP 12+",  color: "#F0997B", bg: "rgba(240,153,123,0.08)", border: "rgba(240,153,123,0.22)" },
  { id: "dolphin", label: "Dolphin", vip: "VIP 7–11", color: "#5DCAA5", bg: "rgba(93,202,165,0.08)",   border: "rgba(93,202,165,0.22)"  },
  { id: "minnow",  label: "Minnow",  vip: "VIP 0–6",  color: "#85B7EB", bg: "rgba(133,183,235,0.08)", border: "rgba(133,183,235,0.22)" },
];

// ── Data loading ──────────────────────────────────────────────────────────────
async function loadJSON(path) {
  try { const r = await fetch(path); if (!r.ok) return null; return await r.json(); }
  catch { return null; }
}
async function loadCSV(path) {
  try {
    const r = await fetch(path); if (!r.ok) return null;
    const text = await r.text();
    const lines = text.trim().split("\n");
    if (lines.length < 2) return [];
    const headers = lines[0].split(",").map(h => h.trim());
    return lines.slice(1).map(line => {
      const vals = line.split(","); const obj = {};
      headers.forEach((h, i) => { const v = vals[i]?.trim(); obj[h] = isNaN(v) || v === "" ? v : Number(v); });
      return obj;
    });
  } catch { return null; }
}

// ── Formatters ────────────────────────────────────────────────────────────────
function fmt(v) {
  if (v == null || isNaN(v)) return "–";
  if (v >= 1e9) return (v / 1e9).toFixed(2) + "B";
  if (v >= 1e6) return (v / 1e6).toFixed(2) + "M";
  if (v >= 1e3) return (v / 1e3).toFixed(1) + "K";
  return Number(v).toLocaleString();
}
function fmtShort(v) {
  if (v == null || isNaN(v)) return "–";
  if (v >= 1e9) return (v / 1e9).toFixed(1) + "B";
  if (v >= 1e6) return (v / 1e6).toFixed(1) + "M";
  if (v >= 1e3) return (v / 1e3).toFixed(0) + "K";
  return Number(v).toLocaleString();
}

// ── Base card ─────────────────────────────────────────────────────────────────
function Card({ children, style }) {
  return (
    <div style={{
      background: "rgba(255,255,255,0.025)",
      border: "1px solid rgba(255,255,255,0.07)",
      borderRadius: 12, ...style,
    }}>{children}</div>
  );
}

function SectionHead({ num, title, desc, color }) {
  return (
    <div style={{ marginBottom: 14 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <div style={{ width: 22, height: 22, borderRadius: 6, background: color + "20", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 11, fontWeight: 700, color, flexShrink: 0 }}>{num}</div>
        <div style={{ fontSize: 14, fontWeight: 600, color: "#e0e0e0" }}>{title}</div>
      </div>
      {desc && <div style={{ fontSize: 11, color: "#555", marginTop: 4, paddingLeft: 32 }}>{desc}</div>}
    </div>
  );
}

// ── Metric card ───────────────────────────────────────────────────────────────
function MetricCard({ label, value, sub, accent }) {
  return (
    <Card style={{ padding: "18px 22px", flex: "1 1 160px", minWidth: 150, ...(accent ? { borderColor: accent + "55" } : {}) }}>
      <div style={{ fontSize: 10, color: "#666", letterSpacing: 0.8, marginBottom: 8, textTransform: "uppercase" }}>{label}</div>
      <div style={{ fontSize: 26, fontWeight: 700, color: accent || "#e8e8e8", letterSpacing: -0.5, lineHeight: 1 }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: "#555", marginTop: 6 }}>{sub}</div>}
    </Card>
  );
}

// ── Sparkline ─────────────────────────────────────────────────────────────────
function Spark({ vals, color, h = 38 }) {
  if (!vals || vals.length < 2) return null;
  const clean = vals.filter(v => v != null);
  if (clean.length < 2) return null;
  const mn = Math.min(...clean) * 0.95, mx = Math.max(...clean) * 1.05;
  const rng = mx - mn || 1;
  const W = 200;
  const pts = clean.map((v, i) => `${4 + i * ((W - 8) / (clean.length - 1))},${h - 4 - ((v - mn) / rng) * (h - 8)}`).join(" ");
  return (
    <svg viewBox={`0 0 ${W} ${h}`} style={{ width: "100%", display: "block" }} preserveAspectRatio="none">
      <polyline points={pts} fill="none" stroke={color} strokeWidth={1.5} strokeLinecap="round" strokeLinejoin="round" />
      {clean.map((v, i) => (
        <circle key={i} cx={4 + i * ((W - 8) / (clean.length - 1))} cy={h - 4 - ((v - mn) / rng) * (h - 8)} r={2.5} fill={color} />
      ))}
    </svg>
  );
}

// ── Trend chart with hover tooltip ───────────────────────────────────────────
function TrendChart({ data, events }) {
  const [hovIdx, setHovIdx] = useState(null);
  const svgRef = useRef(null);

  const W = 900, H = 230, PL = 56, PR = 20, PT = 28, PB = 36;
  const cw = W - PL - PR, ch = H - PT - PB;

  const all = data.flatMap(d => [d.actual, d.upper, d.lower].filter(v => v != null && v > 0));
  if (!all.length) return <div style={{ color: "#444", textAlign: "center", padding: 40 }}>No data</div>;
  const mx = Math.max(...all) * 1.08;
  const mn = Math.min(...all) * 0.88;
  const rng = mx - mn || 1;

  const xi = i => PL + (i / Math.max(data.length - 1, 1)) * cw;
  const yi = v => PT + (1 - (v - mn) / rng) * ch;

  const handleMouseMove = (e) => {
    const svg = svgRef.current; if (!svg) return;
    const rect = svg.getBoundingClientRect();
    const scaleX = W / rect.width;
    const mouseX = (e.clientX - rect.left) * scaleX;
    const raw = (mouseX - PL) / (cw / Math.max(data.length - 1, 1));
    setHovIdx(Math.max(0, Math.min(data.length - 1, Math.round(raw))));
  };

  const band = data.map((d, i) => `${i === 0 ? "M" : "L"}${xi(i)},${yi(d.upper || d.baseline)}`).join(" ") +
    data.slice().reverse().map((d, i) => `L${xi(data.length - 1 - i)},${yi(d.lower || d.baseline)}`).join(" ") + "Z";
  const bl = data.map((d, i) => `${i === 0 ? "M" : "L"}${xi(i)},${yi(d.baseline)}`).join(" ");
  const ac = data.map((d, i) => `${i === 0 ? "M" : "L"}${xi(i)},${yi(d.actual)}`).join(" ");

  // Event regions from events.json overlaid on the 30-day window
  const eventBands = (events || []).map(ev => {
    const s = data.findIndex(d => d.ds >= ev.start_date);
    const e = data.findLastIndex ? data.findLastIndex(d => d.ds <= ev.end_date) : (() => { let idx = -1; data.forEach((d, i) => { if (d.ds <= ev.end_date) idx = i; }); return idx; })();
    if (s < 0 || e < 0 || s > e) return null;
    return { xi_s: xi(s), xi_e: xi(e), name: ev.name || "Event" };
  }).filter(Boolean);

  const hov = hovIdx != null ? data[hovIdx] : null;
  const hovX = hov ? xi(hovIdx) : 0;
  const hovDev = hov && hov.baseline ? ((hov.actual - hov.baseline) / hov.baseline * 100) : null;
  const hovDevColor = hovDev == null ? "#666" : Math.abs(hovDev) < 15 ? "#5DCAA5" : Math.abs(hovDev) < 25 ? "#EF9F27" : "#F0997B";
  // Flip tooltip to left side when near right edge
  const tipLeft = hov && hovX > W * 0.65;
  const tipX = tipLeft ? hovX - 148 : hovX + 10;

  return (
    <svg ref={svgRef} viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", display: "block", cursor: "crosshair" }}
      onMouseMove={handleMouseMove} onMouseLeave={() => setHovIdx(null)}>
      {/* Grid lines */}
      {[0.2, 0.4, 0.6, 0.8].map(f => {
        const val = mn + rng * (1 - f);
        return (
          <g key={f}>
            <line x1={PL} y1={PT + f * ch} x2={W - PR} y2={PT + f * ch} stroke="rgba(255,255,255,0.05)" strokeWidth={0.5} />
            <text x={PL - 8} y={PT + f * ch + 4} textAnchor="end" fontSize={10} fill="#444">
              {fmtShort(val)}
            </text>
          </g>
        );
      })}
      {/* Event bands */}
      {eventBands.map((eb, i) => (
        <g key={i}>
          <rect x={eb.xi_s} y={PT} width={Math.max(eb.xi_e - eb.xi_s, 4)} height={ch} fill="rgba(239,159,39,0.07)" />
          <text x={(eb.xi_s + eb.xi_e) / 2} y={PT + 12} textAnchor="middle" fontSize={9} fill="#EF9F27" opacity={0.8}>{eb.name}</text>
        </g>
      ))}
      {/* Confidence band */}
      <path d={band} fill="rgba(133,183,235,0.07)" />
      {/* Baseline */}
      <path d={bl} fill="none" stroke="rgba(133,183,235,0.28)" strokeWidth={1.2} strokeDasharray="5 4" />
      {/* Actual line */}
      <path d={ac} fill="none" stroke="#85B7EB" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round" />
      {/* Signal dots */}
      {data.map((d, i) => d.signal && d.signal !== "ok" ? (
        <circle key={i} cx={xi(i)} cy={yi(d.actual)} r={4} fill={d.signal === "over" ? "#5DCAA5" : "#F0997B"} stroke="#0c0c0e" strokeWidth={1} />
      ) : null)}
      {/* X-axis labels */}
      {data.filter((_, i) => i % 5 === 0 || i === data.length - 1).map(d => {
        const i = data.indexOf(d);
        return <text key={i} x={xi(i)} y={H - 8} textAnchor="middle" fontSize={10} fill="#444">{d.label}</text>;
      })}
      {/* Hover crosshair */}
      {hov && (
        <g>
          <line x1={hovX} y1={PT} x2={hovX} y2={PT + ch} stroke="rgba(255,255,255,0.15)" strokeWidth={1} strokeDasharray="3 3" />
          <circle cx={hovX} cy={yi(hov.actual)} r={5} fill="#85B7EB" stroke="#0c0c0e" strokeWidth={2} />
          {hov.baseline > 0 && <circle cx={hovX} cy={yi(hov.baseline)} r={3.5} fill="none" stroke="rgba(133,183,235,0.5)" strokeWidth={1.5} />}
          {/* Tooltip box */}
          <g transform={`translate(${tipX},${Math.max(PT + 4, yi(hov.actual) - 70)})`}>
            <rect x={0} y={0} width={138} height={78} rx={6} fill="#1a1a1f" stroke="rgba(255,255,255,0.12)" strokeWidth={0.8} />
            <text x={10} y={18} fontSize={10} fill="#888">{hov.ds}</text>
            <text x={10} y={36} fontSize={11} fill="#ccc">Actual  <tspan fontWeight="700" fill="#85B7EB">{fmtShort(hov.actual)}</tspan></text>
            <text x={10} y={52} fontSize={11} fill="#ccc">Base    <tspan fontWeight="700" fill="#555">{fmtShort(hov.baseline)}</tspan></text>
            <text x={10} y={68} fontSize={11} fill="#ccc">Dev     <tspan fontWeight="700" fill={hovDevColor}>{hovDev != null ? (hovDev > 0 ? "+" : "") + hovDev.toFixed(1) + "%" : "–"}</tspan></text>
          </g>
        </g>
      )}
      {/* Invisible overlay for mouse events */}
      <rect x={PL} y={PT} width={cw} height={ch} fill="transparent" />
    </svg>
  );
}

// ── Event timeline strip ──────────────────────────────────────────────────────
function EventTimeline({ events }) {
  if (!events || events.length === 0) {
    return (
      <Card style={{ padding: "16px 20px" }}>
        <div style={{ fontSize: 12, color: "#444", textAlign: "center" }}>
          No events loaded — add an event calendar xlsx to <code>event_calendar/</code> and re-run the pipeline.
        </div>
      </Card>
    );
  }
  const sorted = [...events].sort((a, b) => a.start_date.localeCompare(b.start_date)).slice(-20);
  return (
    <Card style={{ padding: "6px 0", overflowX: "auto" }}>
      <div style={{ display: "flex", flexDirection: "column", gap: 0 }}>
        {sorted.map((ev, i) => {
          const start = new Date(ev.start_date);
          const end   = new Date(ev.end_date);
          const days  = Math.round((end - start) / 864e5) + 1;
          const past  = end < new Date();
          return (
            <div key={i} style={{ display: "grid", gridTemplateColumns: "130px 110px 40px 1fr", padding: "9px 20px", alignItems: "center", borderBottom: i < sorted.length - 1 ? "1px solid rgba(255,255,255,0.05)" : "none", opacity: past ? 0.5 : 1 }}>
              <span style={{ fontSize: 11, fontWeight: 600, color: past ? "#555" : "#EF9F27" }}>{ev.name || "Event"}</span>
              <span style={{ fontSize: 11, color: "#555" }}>{ev.start_date} → {ev.end_date}</span>
              <span style={{ fontSize: 10, color: "#444" }}>{days}d</span>
              <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
                {(ev.cohorts || []).map(c => {
                  const co = COHORTS.find(x => x.id === c);
                  return co ? <span key={c} style={{ fontSize: 9, padding: "2px 6px", borderRadius: 4, background: co.bg, color: co.color, border: `1px solid ${co.border}` }}>{co.label}</span> : null;
                })}
                {ev.type && <span style={{ fontSize: 9, padding: "2px 6px", borderRadius: 4, background: "rgba(239,159,39,0.1)", color: "#EF9F27", border: "1px solid rgba(239,159,39,0.2)" }}>{ev.type}</span>}
              </div>
            </div>
          );
        })}
      </div>
    </Card>
  );
}

// ── Main Dashboard ─────────────────────────────────────────────────────────────
function Dashboard() {
  const [metrics,  setMetrics]  = useState(null);
  const [forecast, setForecast] = useState(null);
  const [alerts,   setAlerts]   = useState(null);
  const [events,   setEvents]   = useState(null);
  const [loading,  setLoading]  = useState(true);
  const [error,    setError]    = useState(null);

  useEffect(() => {
    Promise.all([
      loadCSV("/data/daily_metrics.csv"),
      loadJSON("/data/forecast.json"),
      loadJSON("/data/alerts.json"),
      loadJSON("/event_calendar/events.json"),
    ]).then(([m, f, a, ev]) => {
      if (!m) setError("data/daily_metrics.csv not found — run the pipeline first.");
      setMetrics(m); setForecast(f); setAlerts(a); setEvents(Array.isArray(ev) ? ev : []);
      setLoading(false);
    });
  }, []);

  const EMPTY = {
    trendData: [], ydAll: {},
    cohortRows: COHORTS.map(c => ({ c })),
    velData:    COHORTS.map(c => ({ c, vals: [], trend: "stable" })),
    spenderData:COHORTS.map(c => ({ c, vals: [], newVals: [] })),
    reportDate: "–", dataDate: "–",
  };

  const { trendData, ydAll, cohortRows, velData, spenderData, reportDate, dataDate } = useMemo(() => {
    if (!metrics || !metrics.length) return EMPTY;

    const allDates  = [...new Set(metrics.map(r => r.ds))].sort();
    const latestDate= allDates[allDates.length - 1];
    const last30    = allDates.slice(-30);
    const last7     = allDates.slice(-7);

    const byDate = {};
    metrics.forEach(r => {
      if (!byDate[r.ds]) byDate[r.ds] = { actual: 0, baseline: 0, upper: 0, lower: 0, is_event: false, signals: [] };
      byDate[r.ds].actual   += r.total_gold_spent || 0;
      byDate[r.ds].baseline += r.baseline_p50      || 0;
      byDate[r.ds].upper    += r.baseline_upper    || 0;
      byDate[r.ds].lower    += r.baseline_lower    || 0;
      if (r.event_flag) byDate[r.ds].is_event = true;
      if (r.signal) byDate[r.ds].signals.push(r.signal);
    });

    const trendData = last30.map(ds => {
      const d   = new Date(ds);
      const row = byDate[ds] || {};
      const sig = row.signals || [];
      return {
        ds, label: `${d.getDate()}/${d.getMonth() + 1}`,
        actual: row.actual, baseline: row.baseline,
        upper: row.upper, lower: row.lower,
        is_event: row.is_event,
        signal: sig.includes("over") ? "over" : sig.includes("under") ? "under" : "ok",
      };
    });

    const ydAll     = byDate[latestDate] || {};
    const cohortRows= COHORTS.map(c => ({ c, ...(metrics.find(r => r.ds === latestDate && r.cohort === c.id) || {}) }));
    const velData   = COHORTS.map(c => {
      const vals  = last7.map(ds => metrics.find(m => m.ds === ds && m.cohort === c.id)?.balance_velocity ?? null);
      const trend = vals.length >= 2 && (vals.filter(v => v != null).slice(-1)[0] ?? 0) < (vals.filter(v => v != null)[0] ?? 0) ? "declining" : "stable";
      return { c, vals, trend };
    });
    const spenderData = COHORTS.map(c => ({
      c,
      vals:    last7.map(ds => metrics.find(m => m.ds === ds && m.cohort === c.id)?.active_spenders ?? null),
      newVals: last7.map(ds => metrics.find(m => m.ds === ds && m.cohort === c.id)?.new_spenders    ?? null),
    }));

    const nd = new Date(latestDate); nd.setDate(nd.getDate() + 1);
    return {
      trendData, ydAll, cohortRows, velData, spenderData,
      reportDate: nd.toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" }),
      dataDate:   new Date(latestDate).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" }),
    };
  }, [metrics]);

  if (loading) return <div style={{ fontFamily: "monospace", color: "#444", padding: 60, background: "#0c0c0e", minHeight: "100vh" }}>Loading…</div>;
  if (error)   return <div style={{ fontFamily: "monospace", color: "#F0997B", padding: 60, background: "#0c0c0e", minHeight: "100vh" }}><strong>Pipeline data not available</strong><br /><br />{error}</div>;

  const ydActual   = ydAll?.actual   || 0;
  const ydBaseline = ydAll?.baseline || 0;
  const deviation  = ydBaseline ? ((ydActual - ydBaseline) / ydBaseline * 100).toFixed(1) : null;
  const devAbs     = parseFloat(deviation);
  const devColor   = isNaN(devAbs) ? "#666" : Math.abs(devAbs) > 25 ? "#F0997B" : Math.abs(devAbs) > 15 ? "#EF9F27" : "#5DCAA5";
  const totalSpenders = cohortRows.reduce((s, r) => s + (r.active_spenders || 0), 0);

  const forecastRows = (() => {
    const dates = (forecast?.cohorts?.whale || []).map(r => r.date);
    if (!dates.length) return [];
    return dates.map(ds => {
      const w = (forecast?.cohorts?.whale   || []).find(r => r.date === ds) || {};
      const d2= (forecast?.cohorts?.dolphin || []).find(r => r.date === ds) || {};
      const mn= (forecast?.cohorts?.minnow  || []).find(r => r.date === ds) || {};
      const d = new Date(ds);
      return {
        date: `${d.getDate()}/${d.getMonth()+1} (${["Sun","Mon","Tue","Wed","Thu","Fri","Sat"][d.getDay()]})`,
        forecast: (w.forecast||0) + (d2.forecast||0) + (mn.forecast||0),
        lower:    (w.lower||0)    + (d2.lower||0)    + (mn.lower||0),
        upper:    (w.upper||0)    + (d2.upper||0)    + (mn.upper||0),
        conf: w.confidence || "–", is_event: w.is_event || d2.is_event || mn.is_event,
      };
    });
  })();

  const alertList = alerts?.alerts || [];

  // ── Render ──────────────────────────────────────────────────────────────────
  return (
    <div style={{
      fontFamily: "'JetBrains Mono','SF Mono','Fira Code',monospace",
      color: "#d0d0d0", background: "#0c0c0e",
      maxWidth: 1480, margin: "0 auto", padding: "28px 32px",
    }}>

      {/* ── Header ── */}
      <div style={{ borderBottom: "1px solid rgba(255,255,255,0.07)", paddingBottom: 22, marginBottom: 28, display: "flex", alignItems: "flex-start", justifyContent: "space-between", flexWrap: "wrap", gap: 12 }}>
        <div>
          <div style={{ display: "flex", alignItems: "baseline", gap: 12, marginBottom: 6 }}>
            <span style={{ fontSize: 11, fontWeight: 700, color: "#EF9F27", letterSpacing: 2, textTransform: "uppercase" }}>KTO</span>
            <span style={{ fontSize: 12, color: "#555" }}>Kiếm Thế Origin</span>
          </div>
          <div style={{ fontSize: 26, fontWeight: 700, color: "#f0f0f0", letterSpacing: -0.5, marginBottom: 8 }}>
            Daily spending baseline
          </div>
          <div style={{ display: "flex", gap: 18, fontSize: 11, color: "#444", flexWrap: "wrap" }}>
            <span>Report: <span style={{ color: "#666" }}>{reportDate}</span></span>
            <span style={{ color: "#222" }}>|</span>
            <span>Data: <span style={{ color: "#666" }}>{dataDate} (D-1)</span></span>
            <span style={{ color: "#222" }}>|</span>
            <span>Model: <span style={{ color: "#666" }}>ETS Holt-Winters · weekly seasonality</span></span>
          </div>
        </div>
        {/* Alert badge */}
        {alertList.length > 0 && (
          <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "8px 14px", borderRadius: 8, background: "rgba(240,153,123,0.08)", border: "1px solid rgba(240,153,123,0.2)" }}>
            <span style={{ fontSize: 13, color: "#F0997B" }}>!</span>
            <span style={{ fontSize: 12, color: "#ccc" }}>{alertList.filter(a => a.type === "warning").length} warning{alertList.filter(a => a.type === "warning").length !== 1 ? "s" : ""}</span>
          </div>
        )}
      </div>

      {/* ── S1: KPI row ── */}
      <div style={{ display: "flex", gap: 12, marginBottom: 28, flexWrap: "wrap" }}>
        <MetricCard label="Actual spend (all cohorts)" value={fmt(ydActual)} sub="Gold burned · D-1" />
        <MetricCard label="ETS baseline (P50)"         value={fmt(ydBaseline)} sub="Expected normal spend" />
        <MetricCard label="Deviation vs baseline"      value={deviation != null ? `${deviation > 0 ? "+" : ""}${deviation}%` : "–"} accent={devColor}
          sub={isNaN(devAbs) ? "–" : Math.abs(devAbs) < 15 ? "Within ±15% band" : Math.abs(devAbs) < 25 ? "Elevated — watch" : "Outside band — investigate"} />
        <MetricCard label="Active Gold spenders"       value={totalSpenders.toLocaleString()} sub="Unique roles · D-1" />
      </div>

      {/* ── S2+S6: Trend chart + Alerts side by side ── */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 320px", gap: 16, marginBottom: 28, alignItems: "start" }}>
        <div>
          <SectionHead num="2" title="30-day spend trend" desc="All cohorts combined. Hover on any point to see actual vs baseline." color="#85B7EB" />
          <div style={{ display: "flex", gap: 16, fontSize: 11, color: "#555", marginBottom: 10, paddingLeft: 32, flexWrap: "wrap" }}>
            <span><span style={{ color: "#85B7EB" }}>——</span> actual</span>
            <span><span style={{ color: "rgba(133,183,235,0.4)" }}>- -</span> baseline</span>
            <span style={{ color: "#5DCAA5" }}>● over</span>
            <span style={{ color: "#F0997B" }}>● under</span>
            {(events || []).length > 0 && <span style={{ color: "#EF9F27" }}>▓ event</span>}
          </div>
          <Card style={{ padding: "16px 12px 8px" }}>
            {trendData.length > 1
              ? <TrendChart data={trendData} events={events || []} />
              : <div style={{ color: "#444", textAlign: "center", padding: 32 }}>Need 30+ days of data</div>}
          </Card>
        </div>

        <div>
          <SectionHead num="6" title="Alerts & signals" desc="Auto-generated from pattern rules." color="#F0997B" />
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {alertList.length === 0
              ? <Card style={{ padding: "16px", fontSize: 12, color: "#555", textAlign: "center" }}>All metrics normal</Card>
              : alertList.map((a, i) => {
                  const col = {
                    warning: { bg: "rgba(240,153,123,0.06)", border: "rgba(240,153,123,0.18)", icon: "#F0997B", sym: "!" },
                    success: { bg: "rgba(93,202,165,0.06)",  border: "rgba(93,202,165,0.18)",  icon: "#5DCAA5", sym: "✓" },
                    info:    { bg: "rgba(133,183,235,0.04)", border: "rgba(133,183,235,0.12)", icon: "#85B7EB", sym: "i" },
                  }[a.type] || { bg: "rgba(255,255,255,0.02)", border: "rgba(255,255,255,0.06)", icon: "#666", sym: "·" };
                  const co = a.cohort ? COHORTS.find(c => c.id === a.cohort) : null;
                  return (
                    <div key={i} style={{ padding: "12px 14px", borderRadius: 10, background: col.bg, border: `1px solid ${col.border}` }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
                        <div style={{ width: 18, height: 18, borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 10, fontWeight: 700, flexShrink: 0, background: col.border, color: col.icon }}>{col.sym}</div>
                        {co && <span style={{ fontSize: 10, fontWeight: 600, padding: "1px 6px", borderRadius: 4, background: co.bg, color: co.color, border: `1px solid ${co.border}` }}>{co.label}</span>}
                        <strong style={{ fontSize: 11, color: "#ccc" }}>{a.title}</strong>
                      </div>
                      <div style={{ fontSize: 11, color: "#666", lineHeight: 1.5, paddingLeft: 26 }}>{a.message}</div>
                    </div>
                  );
                })
            }
          </div>
        </div>
      </div>

      {/* ── S3: Cohort breakdown table ── */}
      <div style={{ marginBottom: 28 }}>
        <SectionHead num="3" title="Cohort breakdown" desc="Yesterday's spend, velocity, and spender count by VIP tier." color="#AFA9EC" />
        <Card>
          <div style={{ display: "grid", gridTemplateColumns: "200px repeat(6, 1fr)", padding: "10px 20px", fontSize: 10, fontWeight: 600, color: "#444", textTransform: "uppercase", letterSpacing: 0.6, borderBottom: "1px solid rgba(255,255,255,0.06)" }}>
            <span>Cohort</span>
            <span style={{ textAlign: "right" }}>Spend</span>
            <span style={{ textAlign: "right" }}>Baseline</span>
            <span style={{ textAlign: "right" }}>vs Base</span>
            <span style={{ textAlign: "right" }}>Velocity</span>
            <span style={{ textAlign: "right" }}>Avg / role</span>
            <span style={{ textAlign: "right" }}>Spenders</span>
          </div>
          {cohortRows.map((r, i) => {
            const dev = r.baseline_p50 && r.total_gold_spent ? ((r.total_gold_spent - r.baseline_p50) / r.baseline_p50 * 100) : null;
            const devCol = dev == null ? "#555" : dev > 15 ? "#5DCAA5" : dev < -15 ? "#F0997B" : dev < -5 ? "#EF9F27" : "#888";
            return (
              <div key={i} style={{ display: "grid", gridTemplateColumns: "200px repeat(6, 1fr)", padding: "14px 20px", fontSize: 13, alignItems: "center", borderBottom: i < 2 ? "1px solid rgba(255,255,255,0.05)" : "none" }}>
                <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                  <div style={{ width: 9, height: 9, borderRadius: 3, background: r.c.color, flexShrink: 0 }} />
                  <span style={{ fontWeight: 600, color: "#e0e0e0" }}>{r.c.label}</span>
                  <span style={{ fontSize: 10, color: "#444" }}>{r.c.vip}</span>
                </div>
                <span style={{ textAlign: "right", color: "#ccc" }}>{fmt(r.total_gold_spent)}</span>
                <span style={{ textAlign: "right", color: "#555" }}>{fmt(r.baseline_p50)}</span>
                <span style={{ textAlign: "right", fontWeight: 700, color: devCol }}>
                  {dev != null ? (dev > 0 ? "+" : "") + dev.toFixed(1) + "%" : "–"}
                </span>
                <span style={{ textAlign: "right", color: r.balance_velocity != null && r.balance_velocity < 0.4 ? "#F0997B" : "#999" }}>
                  {r.balance_velocity != null ? r.balance_velocity.toFixed(3) : "–"}
                </span>
                <span style={{ textAlign: "right", color: "#888" }}>{fmt(r.avg_spend_per_role)}</span>
                <span style={{ textAlign: "right", color: "#999" }}>
                  {(r.active_spenders || 0).toLocaleString()}
                  {r.new_spenders > 0 && <span style={{ fontSize: 10, color: "#5DCAA5", marginLeft: 5 }}>+{r.new_spenders}</span>}
                </span>
              </div>
            );
          })}
        </Card>
      </div>

      {/* ── S4: Velocity & Spenders — 3-column grid ── */}
      <div style={{ marginBottom: 28 }}>
        <SectionHead num="4" title="Velocity & spender trends (7-day)" desc="Balance velocity = Gold spent ÷ (opening balance + inflows). Lower = hoarding." color="#F0997B" />
        <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12 }}>
          {velData.map((v, i) => {
            const sp         = spenderData[i];
            const latestVel  = v.vals.filter(x => x != null).slice(-1)[0];
            const latestSp   = sp.vals.filter(x => x != null).slice(-1)[0];
            return (
              <Card key={i} style={{ padding: "16px 18px", ...(v.trend === "declining" ? { borderColor: v.c.color + "44" } : {}) }}>
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 12 }}>
                  <div style={{ width: 9, height: 9, borderRadius: 3, background: v.c.color }} />
                  <span style={{ fontSize: 13, fontWeight: 600, color: "#e0e0e0" }}>{v.c.label}</span>
                  <span style={{ fontSize: 10, color: "#444" }}>{v.c.vip}</span>
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 12 }}>
                  <div>
                    <div style={{ fontSize: 10, color: "#555", marginBottom: 3 }}>VELOCITY</div>
                    <div style={{ fontSize: 22, fontWeight: 700, color: v.trend === "declining" ? v.c.color : "#e0e0e0", lineHeight: 1 }}>
                      {latestVel != null ? latestVel.toFixed(3) : "–"}
                    </div>
                    <div style={{ marginTop: 4, display: "inline-block", fontSize: 9, padding: "2px 6px", borderRadius: 4, fontWeight: 600, background: v.trend === "declining" ? v.c.color + "18" : "rgba(255,255,255,0.04)", color: v.trend === "declining" ? v.c.color : "#444" }}>
                      {v.trend}
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: 10, color: "#555", marginBottom: 3 }}>SPENDERS</div>
                    <div style={{ fontSize: 22, fontWeight: 700, color: "#e0e0e0", lineHeight: 1 }}>
                      {latestSp != null ? latestSp.toLocaleString() : "–"}
                    </div>
                  </div>
                </div>
                <Spark vals={v.vals} color={v.c.color} h={40} />
              </Card>
            );
          })}
        </div>
      </div>

      {/* ── S5: Event timeline ── */}
      <div style={{ marginBottom: 28 }}>
        <SectionHead num="5" title="Event calendar" desc="Scheduled in-game events. Event windows are excluded from ETS training to avoid biasing the baseline." color="#EF9F27" />
        <EventTimeline events={events || []} />
      </div>

      {/* ── S7: 7-day forecast ── */}
      <div style={{ marginBottom: 28 }}>
        <SectionHead num="7" title="7-day forecast" desc="ETS projection per cohort summed. Confidence interval widens each day out." color="#AFA9EC" />
        {forecastRows.length === 0
          ? <Card style={{ padding: 20, fontSize: 12, color: "#555", textAlign: "center" }}>Run <code>python pipeline/build_baseline.py</code> to generate forecasts.</Card>
          : (
            <Card>
              <div style={{ display: "grid", gridTemplateColumns: "160px repeat(4, 1fr)", padding: "10px 20px", fontSize: 10, fontWeight: 600, color: "#444", textTransform: "uppercase", letterSpacing: 0.6, borderBottom: "1px solid rgba(255,255,255,0.06)" }}>
                <span>Date</span>
                <span style={{ textAlign: "right" }}>Forecast</span>
                <span style={{ textAlign: "right" }}>Lower</span>
                <span style={{ textAlign: "right" }}>Upper</span>
                <span style={{ textAlign: "right" }}>Conf.</span>
              </div>
              {forecastRows.map((f, i) => (
                <div key={i} style={{ display: "grid", gridTemplateColumns: "160px repeat(4, 1fr)", padding: "11px 20px", fontSize: 12, alignItems: "center", borderBottom: i < forecastRows.length - 1 ? "1px solid rgba(255,255,255,0.05)" : "none", background: f.is_event ? "rgba(239,159,39,0.04)" : "transparent" }}>
                  <span style={{ fontWeight: 500, color: "#ccc" }}>
                    {f.date}
                    {f.is_event && <span style={{ fontSize: 9, padding: "2px 5px", borderRadius: 4, marginLeft: 8, background: "rgba(239,159,39,0.12)", color: "#EF9F27", fontWeight: 600 }}>EVENT</span>}
                  </span>
                  <span style={{ textAlign: "right", color: "#ddd", fontWeight: 600 }}>{fmt(f.forecast)}</span>
                  <span style={{ textAlign: "right", color: "#555" }}>{fmt(f.lower)}</span>
                  <span style={{ textAlign: "right", color: "#555" }}>{fmt(f.upper)}</span>
                  <span style={{ textAlign: "right", fontSize: 11, color: i > 4 ? "#F0997B" : "#666" }}>{f.conf}</span>
                </div>
              ))}
            </Card>
          )
        }
      </div>

      {/* ── Footer ── */}
      <div style={{ fontSize: 10, color: "#333", borderTop: "1px solid rgba(255,255,255,0.04)", paddingTop: 16, lineHeight: 2 }}>
        <span style={{ color: "#555" }}>source</span> hive.kto_658 · moneychange_reduce + jingpai_succ + moneychange_add + recharge_deliver<br />
        <span style={{ color: "#555" }}>filter</span> moneytype=Gold · serverid LIKE '60%' · exclude p2p logways 21,29,64,158,404<br />
        <span style={{ color: "#555" }}>cohort</span> Whale VIP≥12 · Dolphin VIP 7–11 · Minnow VIP 0–6 (TRY_CAST varchar→int)<br />
        <span style={{ color: "#555" }}>model</span> ETS Holt-Winters · α optimized · seasonal period 7 · event windows excluded from training
      </div>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(React.createElement(Dashboard));
