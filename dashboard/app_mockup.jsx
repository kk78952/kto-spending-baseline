import { useState, useMemo } from "react";

// --- DATA GENERATION ---
const COHORTS = [
  { id: "whale", label: "Whale", vip: "VIP 12+", color: "#F0997B", dim: "#4A1B0C", bg: "rgba(240,153,123,0.08)", border: "rgba(240,153,123,0.2)" },
  { id: "dolphin", label: "Dolphin", vip: "VIP 7–11", color: "#5DCAA5", dim: "#04342C", bg: "rgba(93,202,165,0.08)", border: "rgba(93,202,165,0.2)" },
  { id: "minnow", label: "Minnow", vip: "VIP 0–6", color: "#85B7EB", dim: "#042C53", bg: "rgba(133,183,235,0.08)", border: "rgba(133,183,235,0.2)" },
];

function makeTrend() {
  const days = [];
  const base = 850000;
  for (let i = 29; i >= 0; i--) {
    const d = new Date(2026, 3, 7);
    d.setDate(d.getDate() - i);
    const dow = d.getDay();
    const wb = dow === 0 || dow === 6 ? 1.22 : dow === 5 ? 1.14 : 1;
    const trend = 1 - i * 0.0012;
    const noise = 0.92 + Math.random() * 0.16;
    const isEv = i >= 18 && i <= 22;
    const em = isEv ? 1.55 + Math.random() * 0.5 : 1;
    const actual = Math.round(base * wb * trend * noise * em);
    const bl = Math.round(base * wb * trend * (isEv ? 1.75 : 1));
    days.push({ date: d, label: `${d.getDate()}/${d.getMonth() + 1}`, actual, baseline: bl, upper: Math.round(bl * 1.15), lower: Math.round(bl * 0.85), isEvent: isEv, signal: actual > Math.round(bl * 1.15) ? "over" : actual < Math.round(bl * 0.85) ? "under" : "ok" });
  }
  return days;
}

function makeForecast() {
  const b = 840000; const dn = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"]; const rows = [];
  for (let i = 1; i <= 7; i++) {
    const d = new Date(2026, 3, 7 + i); const dow = d.getDay();
    const wb = dow === 0 || dow === 6 ? 1.2 : dow === 5 ? 1.15 : 1;
    const f = Math.round(b * wb * (1 - i * 0.005)); const sp = 0.08 + i * 0.03;
    rows.push({ date: `${d.getDate()}/${d.getMonth()+1} (${dn[dow]})`, forecast: f, lower: Math.round(f*(1-sp)), upper: Math.round(f*(1+sp)), conf: `±${Math.round(sp*100)}%`, isEvent: i===4 });
  }
  return rows;
}

const SHOPS = [
  { name: "Cửa Hàng (ShopBuy)", gold: 412600, pct: 42.3, color: "#85B7EB" },
  { name: "Trân Bảo Hành", gold: 198400, pct: 20.3, color: "#5DCAA5" },
  { name: "Bày Bán (MarketStall)", gold: 142000, pct: 14.6, color: "#EF9F27" },
  { name: "Lì Xì (RedBag)", gold: 94200, pct: 9.7, color: "#F0997B" },
  { name: "Trang Bị", gold: 78500, pct: 8.0, color: "#AFA9EC" },
  { name: "Others", gold: 49800, pct: 5.1, color: "#5F5E5A" },
];

const ALERTS = [
  { t: "warn", msg: "Whale velocity at 0.38 — below 0.40 threshold for 2 consecutive days. Active spenders stable → hoarding signal.", co: "whale" },
  { t: "good", msg: "Dolphin avg spend/role up 12% this week. Driven by TreasureShop purchases.", co: "dolphin" },
  { t: "warn", msg: "New spenders dropped 34% vs 7-day avg. No UA campaign running — organic acquisition slowing.", co: null },
  { t: "info", msg: "Weekend forecast: +18% vs weekday baseline. Normal seasonal pattern.", co: null },
];

// --- COMPONENTS ---
function Fmt({ v }) {
  if (v >= 1e9) return <>{(v/1e9).toFixed(1)}B</>;
  if (v >= 1e6) return <>{(v/1e6).toFixed(1)}M</>;
  if (v >= 1e3) return <>{(v/1e3).toFixed(0)}K</>;
  return <>{v.toLocaleString()}</>;
}

function Metric({ label, value, sub, accent, wide }) {
  return (
    <div style={{ background: "rgba(255,255,255,0.03)", border: accent ? `1px solid ${accent}44` : "1px solid rgba(255,255,255,0.06)", borderRadius: 10, padding: "14px 18px", flex: wide ? "1 1 200px" : "1 1 140px", minWidth: wide ? 200 : 130 }}>
      <div style={{ fontSize: 11, color: "#8a8a8a", letterSpacing: 0.3, marginBottom: 6, textTransform: "uppercase" }}>{label}</div>
      <div style={{ fontSize: 22, fontWeight: 600, color: accent || "#e8e8e8", letterSpacing: -0.5 }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: "#666", marginTop: 4 }}>{sub}</div>}
    </div>
  );
}

function Spark({ vals, color, w = 140, h = 36 }) {
  const mn = Math.min(...vals) - 0.04, mx = Math.max(...vals) + 0.04;
  const pts = vals.map((v, i) => `${4 + i * ((w - 8) / (vals.length - 1))},${h - 4 - ((v - mn) / (mx - mn)) * (h - 8)}`).join(" ");
  return (
    <svg viewBox={`0 0 ${w} ${h}`} style={{ width: "100%", display: "block" }}>
      <polyline points={pts} fill="none" stroke={color} strokeWidth={1.5} strokeLinecap="round" strokeLinejoin="round" />
      {vals.map((v, i) => <circle key={i} cx={4 + i * ((w - 8) / (vals.length - 1))} cy={h - 4 - ((v - mn) / (mx - mn)) * (h - 8)} r={2} fill={color} />)}
    </svg>
  );
}

function TrendChart({ data }) {
  const W = 640, H = 210, PL = 44, PR = 8, PT = 24, PB = 28;
  const cw = W - PL - PR, ch = H - PT - PB;
  const all = data.flatMap(d => [d.actual, d.upper, d.lower]);
  const mx = Math.max(...all) * 1.05, mn = Math.min(...all) * 0.92;
  const x = i => PL + (i / (data.length - 1)) * cw;
  const y = v => PT + (1 - (v - mn) / (mx - mn)) * ch;

  const band = data.map((d, i) => `${i === 0 ? "M" : "L"}${x(i)},${y(d.upper)}`).join(" ") + data.slice().reverse().map((d, i) => `L${x(data.length - 1 - i)},${y(d.lower)}`).join(" ") + "Z";
  const bl = data.map((d, i) => `${i === 0 ? "M" : "L"}${x(i)},${y(d.baseline)}`).join(" ");
  const ac = data.map((d, i) => `${i === 0 ? "M" : "L"}${x(i)},${y(d.actual)}`).join(" ");

  const evStart = data.findIndex(d => d.isEvent);
  const evEnd = data.length - 1 - [...data].reverse().findIndex(d => d.isEvent);

  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", display: "block" }}>
      {[0.25, 0.5, 0.75].map(f => {
        const val = mn + (mx - mn) * (1 - f);
        return (<g key={f}><line x1={PL} y1={PT + f * ch} x2={W - PR} y2={PT + f * ch} stroke="rgba(255,255,255,0.05)" strokeWidth={0.5} /><text x={PL - 6} y={PT + f * ch + 4} textAnchor="end" fontSize={10} fill="#555">{val >= 1e6 ? (val / 1e6).toFixed(1) + "M" : (val / 1e3).toFixed(0) + "K"}</text></g>);
      })}
      {evStart >= 0 && <rect x={x(evStart)} y={PT} width={x(evEnd) - x(evStart)} height={ch} fill="rgba(239,159,39,0.06)" rx={3} />}
      {evStart >= 0 && <text x={(x(evStart) + x(evEnd)) / 2} y={PT + 14} textAnchor="middle" fontSize={10} fill="#EF9F27" fontWeight={500}>Event period</text>}
      <path d={band} fill="rgba(133,183,235,0.07)" />
      <path d={bl} fill="none" stroke="rgba(133,183,235,0.3)" strokeWidth={1} strokeDasharray="4 3" />
      <path d={ac} fill="none" stroke="#85B7EB" strokeWidth={2} />
      {data.map((d, i) => d.signal !== "ok" ? <circle key={i} cx={x(i)} cy={y(d.actual)} r={3.5} fill={d.signal === "over" ? "#5DCAA5" : "#F0997B"} /> : null)}
      {data.filter((_, i) => i % 5 === 0).map(d => { const i = data.indexOf(d); return <text key={i} x={x(i)} y={H - 6} textAnchor="middle" fontSize={10} fill="#555">{d.label}</text>; })}
    </svg>
  );
}

function SectionHead({ num, title, desc, color }) {
  return (
    <div style={{ marginBottom: 14 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <div style={{ width: 22, height: 22, borderRadius: 6, background: color + "18", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 11, fontWeight: 700, color }}>{num}</div>
        <div style={{ fontSize: 15, fontWeight: 600, color: "#e0e0e0" }}>{title}</div>
      </div>
      {desc && <div style={{ fontSize: 12, color: "#666", marginTop: 4, paddingLeft: 32 }}>{desc}</div>}
    </div>
  );
}

// --- MAIN DASHBOARD ---
export default function Dashboard() {
  const trend = useMemo(makeTrend, []);
  const forecast = useMemo(makeForecast, []);
  const yd = trend[trend.length - 1];
  const dev = ((yd.actual - yd.baseline) / yd.baseline * 100).toFixed(1);

  const cohortRows = [
    { c: COHORTS[0], spend: 534800, vel: 0.38, avgSpend: 4920, spenders: 109, newSp: 3, retSp: 106, dev: -14.2 },
    { c: COHORTS[1], spend: 248100, vel: 0.61, avgSpend: 312, spenders: 795, newSp: 41, retSp: 754, dev: 3.1 },
    { c: COHORTS[2], spend: 64300, vel: 0.37, avgSpend: 18, spenders: 3572, newSp: 289, retSp: 3283, dev: -2.0 },
  ];

  const velData = [
    { c: COHORTS[0], vals: [0.52, 0.48, 0.45, 0.43, 0.40, 0.39, 0.38], trend: "declining" },
    { c: COHORTS[1], vals: [0.58, 0.60, 0.62, 0.59, 0.63, 0.60, 0.61], trend: "stable" },
    { c: COHORTS[2], vals: [0.35, 0.37, 0.36, 0.38, 0.37, 0.39, 0.38], trend: "stable" },
  ];

  const spenderData = [
    { c: COHORTS[0], vals: [115, 112, 108, 111, 109, 110, 109], newVals: [4, 2, 3, 5, 3, 2, 3] },
    { c: COHORTS[1], vals: [810, 795, 802, 788, 801, 790, 795], newVals: [38, 42, 35, 40, 44, 39, 41] },
    { c: COHORTS[2], vals: [3600, 3550, 3580, 3520, 3570, 3540, 3572], newVals: [310, 295, 280, 275, 300, 285, 289] },
  ];

  return (
    <div style={{
      fontFamily: "'JetBrains Mono', 'SF Mono', 'Fira Code', monospace",
      color: "#d0d0d0", background: "#0c0c0e", maxWidth: 740, padding: "0 4px",
      "--card": "rgba(255,255,255,0.025)", "--border": "rgba(255,255,255,0.06)",
    }}>
      {/* Header */}
      <div style={{ borderBottom: "1px solid rgba(255,255,255,0.06)", paddingBottom: 20, marginBottom: 28 }}>
        <div style={{ display: "flex", alignItems: "baseline", gap: 12, marginBottom: 6 }}>
          <span style={{ fontSize: 11, fontWeight: 700, color: "#EF9F27", letterSpacing: 2, textTransform: "uppercase" }}>KTO</span>
          <span style={{ fontSize: 11, color: "#555" }}>Kim Thánh Online</span>
        </div>
        <div style={{ fontSize: 24, fontWeight: 700, color: "#f0f0f0", letterSpacing: -0.5, marginBottom: 6 }}>
          Daily spending baseline
        </div>
        <div style={{ display: "flex", gap: 16, fontSize: 11, color: "#555" }}>
          <span>Report: 8 Apr 2026</span>
          <span style={{ color: "#333" }}>|</span>
          <span>Data: 7 Apr 2026 (D-1)</span>
          <span style={{ color: "#333" }}>|</span>
          <span>Model: ETS Holt-Winters</span>
        </div>
      </div>

      {/* S1: Health check */}
      <div style={{ marginBottom: 32 }}>
        <SectionHead num="1" title="Yesterday's health check" desc="Actual Gold spent vs. ETS baseline. Is the economy behaving normally?" color="#85B7EB" />
        <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
          <Metric label="Actual spend" value={<Fmt v={yd.actual} />} sub="All cohorts · Gold" />
          <Metric label="Baseline (P50)" value={<Fmt v={yd.baseline} />} sub="ETS predicted" />
          <Metric label="Deviation" value={`${dev}%`} accent={Math.abs(dev) > 15 ? "#F0997B" : Math.abs(dev) > 8 ? "#EF9F27" : "#5DCAA5"} sub={Math.abs(dev) < 15 ? "Within ±15% band" : "Outside band"} />
          <Metric label="Spender count" value="4,476" sub="↓42 vs 7d avg" />
        </div>
      </div>

      {/* S2: Cohort breakdown */}
      <div style={{ marginBottom: 32 }}>
        <SectionHead num="2" title="Cohort breakdown" desc="Which player segment is driving the deviation? Includes spender headcount and per-role average." color="#AFA9EC" />
        <div style={{ background: "var(--card)", border: "1px solid var(--border)", borderRadius: 10, overflow: "hidden" }}>
          <div style={{ display: "grid", gridTemplateColumns: "minmax(0,1.2fr) repeat(5, minmax(0,1fr))", padding: "10px 16px", fontSize: 10, fontWeight: 600, color: "#555", textTransform: "uppercase", letterSpacing: 0.5, borderBottom: "1px solid var(--border)" }}>
            <span>Cohort</span><span style={{textAlign:"right"}}>Spend</span><span style={{textAlign:"right"}}>Velocity</span><span style={{textAlign:"right"}}>Avg/role</span><span style={{textAlign:"right"}}>Spenders</span><span style={{textAlign:"right"}}>vs Base</span>
          </div>
          {cohortRows.map((r, i) => (
            <div key={i} style={{ display: "grid", gridTemplateColumns: "minmax(0,1.2fr) repeat(5, minmax(0,1fr))", padding: "12px 16px", fontSize: 13, alignItems: "center", borderBottom: i < 2 ? "1px solid var(--border)" : "none" }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <div style={{ width: 8, height: 8, borderRadius: 3, background: r.c.color }} />
                <span style={{ fontWeight: 600, color: "#e0e0e0" }}>{r.c.label}</span>
                <span style={{ fontSize: 10, color: "#555" }}>{r.c.vip}</span>
              </div>
              <span style={{ textAlign: "right", color: "#ccc" }}><Fmt v={r.spend} /></span>
              <span style={{ textAlign: "right", color: r.vel < 0.4 ? "#F0997B" : "#ccc" }}>{r.vel.toFixed(2)}</span>
              <span style={{ textAlign: "right", color: "#999" }}><Fmt v={r.avgSpend} /></span>
              <span style={{ textAlign: "right", color: "#999" }}>
                {r.spenders.toLocaleString()}
                <span style={{ fontSize: 10, color: "#5DCAA5", marginLeft: 4 }}>+{r.newSp}</span>
              </span>
              <span style={{ textAlign: "right", fontWeight: 600, color: r.dev > 0 ? "#5DCAA5" : r.dev < -10 ? "#F0997B" : "#888" }}>
                {r.dev > 0 ? "+" : ""}{r.dev}%
              </span>
            </div>
          ))}
        </div>
      </div>

      {/* S3: 30-day trend */}
      <div style={{ marginBottom: 32 }}>
        <SectionHead num="3" title="30-day trend" color="#85B7EB" />
        <div style={{ display: "flex", gap: 16, fontSize: 11, color: "#555", marginBottom: 10, paddingLeft: 32 }}>
          <span><span style={{ color: "#85B7EB" }}>—</span> actual</span>
          <span><span style={{ color: "#85B7EB", opacity: 0.4 }}>- -</span> baseline</span>
          <span style={{ color: "#5DCAA5" }}>● over</span>
          <span style={{ color: "#F0997B" }}>● under</span>
          <span style={{ color: "#EF9F27" }}>■ event</span>
        </div>
        <div style={{ background: "var(--card)", border: "1px solid var(--border)", borderRadius: 10, padding: "16px 12px 8px" }}>
          <TrendChart data={trend} />
        </div>
      </div>

      {/* S4: Velocity + Spender sparklines side by side */}
      <div style={{ marginBottom: 32 }}>
        <SectionHead num="4" title="Velocity & spender trends (7-day)" desc="Left: balance velocity (% Gold used). Right: spender headcount. Green number = new spenders today." color="#F0997B" />
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          {velData.map((v, i) => {
            const sp = spenderData[i];
            return (
              <div key={i} style={{ display: "flex", gap: 10 }}>
                {/* Velocity card */}
                <div style={{ flex: 1, background: "var(--card)", border: `1px solid ${v.trend === "declining" ? v.c.color + "33" : "var(--border)"}`, borderRadius: 10, padding: "12px 16px" }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 6 }}>
                    <div style={{ width: 8, height: 8, borderRadius: 3, background: v.c.color }} />
                    <span style={{ fontSize: 12, fontWeight: 600, color: "#ccc" }}>{v.c.label}</span>
                    <span style={{ fontSize: 10, color: "#555", marginLeft: "auto" }}>velocity</span>
                  </div>
                  <div style={{ display: "flex", alignItems: "baseline", gap: 6, marginBottom: 4 }}>
                    <span style={{ fontSize: 20, fontWeight: 700, color: v.trend === "declining" ? v.c.color : "#e0e0e0" }}>{v.vals[v.vals.length - 1].toFixed(2)}</span>
                    <span style={{ fontSize: 10, padding: "2px 6px", borderRadius: 4, fontWeight: 600, background: v.trend === "declining" ? v.c.color + "18" : "rgba(255,255,255,0.04)", color: v.trend === "declining" ? v.c.color : "#666" }}>{v.trend}</span>
                  </div>
                  <Spark vals={v.vals} color={v.c.color} />
                </div>
                {/* Spender count card */}
                <div style={{ flex: 1, background: "var(--card)", border: "1px solid var(--border)", borderRadius: 10, padding: "12px 16px" }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 6 }}>
                    <div style={{ width: 8, height: 8, borderRadius: 3, background: v.c.color, opacity: 0.5 }} />
                    <span style={{ fontSize: 12, fontWeight: 600, color: "#ccc" }}>{v.c.label}</span>
                    <span style={{ fontSize: 10, color: "#555", marginLeft: "auto" }}>spenders</span>
                  </div>
                  <div style={{ display: "flex", alignItems: "baseline", gap: 6, marginBottom: 4 }}>
                    <span style={{ fontSize: 20, fontWeight: 700, color: "#e0e0e0" }}>{sp.vals[sp.vals.length - 1].toLocaleString()}</span>
                    <span style={{ fontSize: 10, color: "#5DCAA5", fontWeight: 600 }}>+{sp.newVals[sp.newVals.length - 1]} new</span>
                  </div>
                  <Spark vals={sp.vals} color={v.c.color + "88"} />
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* S5: Shop breakdown */}
      <div style={{ marginBottom: 32 }}>
        <SectionHead num="5" title="Gold sinks by shop" desc="Where Gold was spent yesterday — from logway_name groups. Player transfers (Trade, MarketStall) excluded." color="#EF9F27" />
        <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
          {SHOPS.map((s, i) => (
            <div key={i} style={{ display: "flex", alignItems: "center", gap: 12, fontSize: 12, padding: "6px 0" }}>
              <span style={{ width: 160, color: "#aaa", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{s.name}</span>
              <div style={{ flex: 1, height: 6, background: "rgba(255,255,255,0.04)", borderRadius: 3, overflow: "hidden" }}>
                <div style={{ height: "100%", width: `${s.pct / SHOPS[0].pct * 100}%`, background: s.color, borderRadius: 3, opacity: 0.7 }} />
              </div>
              <span style={{ width: 50, textAlign: "right", color: "#777", fontSize: 11 }}><Fmt v={s.gold} /></span>
              <span style={{ width: 36, textAlign: "right", color: "#555", fontSize: 11 }}>{s.pct}%</span>
            </div>
          ))}
        </div>
      </div>

      {/* S6: Alerts */}
      <div style={{ marginBottom: 32 }}>
        <SectionHead num="6" title="Alerts & signals" desc="Pattern-based warnings combining spend, velocity, and spender trends across multiple days." color="#F0997B" />
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {ALERTS.map((a, i) => {
            const colors = { warn: { bg: "rgba(240,153,123,0.06)", border: "rgba(240,153,123,0.15)", icon: "#F0997B", sym: "!" }, good: { bg: "rgba(93,202,165,0.06)", border: "rgba(93,202,165,0.15)", icon: "#5DCAA5", sym: "✓" }, info: { bg: "rgba(133,183,235,0.04)", border: "rgba(133,183,235,0.1)", icon: "#85B7EB", sym: "i" } }[a.t];
            const co = a.co ? COHORTS.find(c => c.id === a.co) : null;
            return (
              <div key={i} style={{ display: "flex", gap: 12, padding: "12px 16px", borderRadius: 10, alignItems: "flex-start", background: colors.bg, border: `1px solid ${colors.border}` }}>
                <div style={{ width: 20, height: 20, borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 11, fontWeight: 700, flexShrink: 0, background: colors.border, color: colors.icon }}>{colors.sym}</div>
                <div style={{ fontSize: 12, lineHeight: 1.6, color: "#999" }}>
                  {co && <span style={{ fontSize: 10, fontWeight: 600, padding: "2px 6px", borderRadius: 4, marginRight: 6, background: co.bg, color: co.color, border: `1px solid ${co.border}` }}>{co.label}</span>}
                  {a.msg}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* S7: Forecast */}
      <div style={{ marginBottom: 32 }}>
        <SectionHead num="7" title="7-day forecast" desc="ETS projection from yesterday's data. Event multiplier applied where scheduled. Confidence widens daily." color="#AFA9EC" />
        <div style={{ background: "var(--card)", border: "1px solid var(--border)", borderRadius: 10, overflow: "hidden" }}>
          <div style={{ display: "grid", gridTemplateColumns: "minmax(0,1.4fr) repeat(4, minmax(0,1fr))", padding: "10px 16px", fontSize: 10, fontWeight: 600, color: "#555", textTransform: "uppercase", letterSpacing: 0.5, borderBottom: "1px solid var(--border)" }}>
            <span>Date</span><span style={{textAlign:"right"}}>Forecast</span><span style={{textAlign:"right"}}>Lower</span><span style={{textAlign:"right"}}>Upper</span><span style={{textAlign:"right"}}>Conf.</span>
          </div>
          {forecast.map((f, i) => (
            <div key={i} style={{
              display: "grid", gridTemplateColumns: "minmax(0,1.4fr) repeat(4, minmax(0,1fr))",
              padding: "10px 16px", fontSize: 12, alignItems: "center",
              borderBottom: i < forecast.length - 1 ? "1px solid var(--border)" : "none",
              background: f.isEvent ? "rgba(239,159,39,0.04)" : "transparent",
            }}>
              <span style={{ fontWeight: 500, color: "#ccc" }}>
                {f.date}
                {f.isEvent && <span style={{ fontSize: 9, padding: "2px 5px", borderRadius: 4, marginLeft: 6, background: "rgba(239,159,39,0.12)", color: "#EF9F27", fontWeight: 600, letterSpacing: 0.3 }}>EVENT</span>}
              </span>
              <span style={{ textAlign: "right", color: "#ddd" }}><Fmt v={f.forecast} /></span>
              <span style={{ textAlign: "right", color: "#555" }}><Fmt v={f.lower} /></span>
              <span style={{ textAlign: "right", color: "#555" }}><Fmt v={f.upper} /></span>
              <span style={{ textAlign: "right", fontSize: 11, color: i > 4 ? "#F0997B" : "#666" }}>{f.conf}</span>
            </div>
          ))}
        </div>
        <div style={{ fontSize: 10, color: "#444", marginTop: 8, paddingLeft: 32 }}>
          Beyond day 14 → switch to scenario planning (see docs/project_spec.md)
        </div>
      </div>

      {/* Footer */}
      <div style={{ fontSize: 10, color: "#333", borderTop: "1px solid rgba(255,255,255,0.04)", paddingTop: 14, lineHeight: 1.8, fontFamily: "var(--font-mono)" }}>
        <span style={{ color: "#555" }}>src</span> hive.kto_658 · moneychange_reduce + moneychange_add + recharge_deliver<br/>
        <span style={{ color: "#555" }}>filter</span> moneytype=Gold · price/100 · exclude LogWay_Trade, MarketStall transfers<br/>
        <span style={{ color: "#555" }}>cohort</span> Whale VIP≥12 · Dolphin VIP 7–11 · Minnow VIP 0–6<br/>
        <span style={{ color: "#555" }}>model</span> ETS Holt-Winters · α=0.2 · weekly seasonality · event calendar excluded from training
      </div>
    </div>
  );
}
