import { FormEvent, useEffect, useMemo, useRef, useState } from "react";

type Json = Record<string, unknown>;
type Session = {
  authenticated: boolean;
  subject: string;
  tenant_id: string;
  role: string;
  actor_type: string;
  auth_method: string;
  resource_acl_enabled: boolean;
};

const pages = [
  ["agents", "Agent／ACL"],
  ["skills", "Skill／匯入"],
  ["automations", "自動任務"],
  ["goals", "Goal／核准"],
  ["planning", "計畫／預測"],
  ["history", "搜尋／稽核"],
] as const;

function cookie(name: string): string {
  const prefix = `${name}=`;
  const row = document.cookie.split("; ").find((item) => item.startsWith(prefix));
  return row ? decodeURIComponent(row.slice(prefix.length)) : "";
}

async function api(path: string, init: RequestInit = {}): Promise<unknown> {
  const headers = new Headers(init.headers);
  if (init.body && !headers.has("content-type")) headers.set("content-type", "application/json");
  const csrf = cookie("gateway_csrf");
  if (csrf && !["GET", "HEAD", "OPTIONS"].includes(init.method ?? "GET")) {
    headers.set("x-csrf-token", csrf);
  }
  const response = await fetch(path, { ...init, headers, credentials: "same-origin" });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = typeof (body as Json).detail === "string" ? (body as Json).detail : response.statusText;
    throw new Error(`${response.status} ${message}`);
  }
  return body;
}

function JsonView({ value }: { value: unknown }) {
  return <pre className="json">{JSON.stringify(value, null, 2)}</pre>;
}

function Presence({ kind, resourceId }: { kind: string; resourceId: string }) {
  const [enabled, setEnabled] = useState(false);
  const [state, setState] = useState("viewing");
  const [peers, setPeers] = useState<unknown[]>([]);
  const path = `/v1/collaboration/presence/${kind}/${encodeURIComponent(resourceId)}`;
  useEffect(() => {
    if (!enabled) return;
    const refresh = () => {
      void api(path, { method: "PUT", body: JSON.stringify({ state }) });
      void api(path).then((value) => setPeers(((value as Json).presence as unknown[]) ?? []));
    };
    refresh();
    const timer = window.setInterval(refresh, 20_000);
    return () => { window.clearInterval(timer); void api(path, { method: "DELETE" }); };
  }, [enabled, state, path]);
  return <div className="presence"><label><input type="checkbox" checked={enabled} onChange={(event) => setEnabled(event.target.checked)}/> 主動顯示我的 presence</label><select value={state} onChange={(event) => setState(event.target.value)} disabled={!enabled}><option value="viewing">檢視中</option><option value="editing">編輯中</option></select><span>{enabled ? `${peers.length} 人在線；僅供提示，不是鎖定` : "預設不送出 presence"}</span></div>;
}

function AccessControl({ kind, resourceId }: { kind: string; resourceId: string }) {
  const [record, setRecord] = useState<Json | null>(null);
  const [grants, setGrants] = useState("[]");
  const [error, setError] = useState("");
  const path = `/v1/access/${kind}/${encodeURIComponent(resourceId)}`;
  async function load() {
    try {
      const value = await api(path) as Json;
      setRecord(value);
      setGrants(JSON.stringify(value.grants ?? [], null, 2));
      setError("");
    } catch (err) { setError(String(err)); }
  }
  useEffect(() => { void load(); }, [path]);
  async function save() {
    if (!record) return;
    try {
      setRecord(await api(path, { method: "PUT", body: JSON.stringify({ expected_version: record.version, grants: JSON.parse(grants) }) }) as Json);
      await load();
    } catch (err) { setError(String(err)); }
  }
  return <details className="acl"><summary>Owner／明確分享 ACL</summary>{record && <p>Owner：{String((record.owner as Json)?.subject ?? "-")} · version {String(record.version)}</p>}<textarea value={grants} onChange={(event) => setGrants(event.target.value)} spellCheck={false}/><button onClick={save} disabled={!record}>以 CAS 更新 grants</button>{error && <p className="error">{error}</p>}</details>;
}

function Login() {
  const [config, setConfig] = useState<Json>({});
  const [error, setError] = useState("");
  useEffect(() => {
    void api("/auth/config").then((value) => setConfig(value as Json)).catch((err) => setError(String(err)));
  }, []);
  async function devLogin() {
    setError("");
    try {
      const response = await fetch("/auth/development-login", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ subject: "development", tenant_id: "default", role: "admin" }),
        redirect: "manual",
      });
      if (!response.ok && response.type !== "opaqueredirect") throw new Error(`登入失敗：${response.status}`);
      window.location.reload();
    } catch (err) {
      setError(String(err));
    }
  }
  return (
    <main className="login-shell">
      <section className="login-card">
        <p className="eyebrow">AI OPS GATEWAY</p>
        <h1>營運控制台</h1>
        <p>登入後才能檢視租戶資源。核准連結不含任何核准憑證。</p>
        {config.oidc_enabled === true && <a className="primary button" href="/auth/login">使用組織帳號登入</a>}
        {config.development_login_enabled === true && <button onClick={devLogin}>開發環境登入</button>}
        {!config.oidc_enabled && !config.development_login_enabled && <p className="notice">管理者尚未設定瀏覽器登入。</p>}
        {error && <p className="error">{error}</p>}
      </section>
    </main>
  );
}

const resourceConfig: Record<string, { path: string; key: string; template?: Json }> = {
  agents: {
    path: "/v1/agents",
    key: "agents",
    template: {
      name: "資安分析 Agent",
      description: "分析 ATR finding 並提出證據導向建議",
      persona: "只根據可驗證證據工作，清楚標示不確定性。",
      role: "worker",
      enabled: true,
      delegate_agent_ids: [],
      skills: [],
      planning_skills: [],
      skill_selection_rules: [],
      policy: { allowed_tools: [], allowed_domains: [], token_budget: 4000 },
    },
  },
  skills: {
    path: "/v1/skills",
    key: "skills",
    template: {
      name: "incident-review",
      display_name: "事故證據覆核",
      description: "依已核可資料整理事故證據",
      instructions: "只引用可驗證證據，標示限制與不確定性。",
      required_tools: [],
      agent_roles: ["chief", "worker"],
      tags: ["incident"],
      enabled: true,
    },
  },
  automations: {
    path: "/v1/automations",
    key: "automations",
    template: {
      name: "每日 ATR 摘要",
      task_type: "atr_summary",
      agent_id: null,
      goal: { objective: "彙整昨日 ATR findings", steps: [] },
      trigger: { kind: "schedule", cron: "0 9 * * *", timezone: "Asia/Taipei", event_types: [], rule_ids: [], severities: [] },
      policy: { allowed_tools: [], allowed_domains: [], max_runs_per_day: 24, token_budget: 20000, approval_timeout_seconds: 86400 },
      enabled: true,
    },
  },
  goals: { path: "/v1/goals", key: "goals" },
};

function ResourcePage({ kind, aclEnabled }: { kind: string; aclEnabled: boolean }) {
  const config = resourceConfig[kind];
  const [rows, setRows] = useState<unknown[]>([]);
  const [selected, setSelected] = useState<unknown>(null);
  const [draft, setDraft] = useState(JSON.stringify(config.template ?? {}, null, 2));
  const [error, setError] = useState("");
  async function load() {
    setError("");
    try {
      const value = (await api(config.path)) as Json;
      setRows((value[config.key] as unknown[]) ?? []);
    } catch (err) {
      setError(String(err));
    }
  }
  useEffect(() => {
    setSelected(null);
    setDraft(JSON.stringify(config.template ?? {}, null, 2));
    void load();
  }, [kind]);
  async function create(event: FormEvent) {
    event.preventDefault();
    try {
      setSelected(await api(config.path, { method: "POST", body: JSON.stringify(JSON.parse(draft)) }));
      await load();
    } catch (err) {
      setError(String(err));
    }
  }
  const selectedRecord = selected as Json | null;
  const resourceKind = ({ agents: "agent", skills: "skill", automations: "automation", goals: "goal" } as Record<string, string>)[kind];
  const resourceId = selectedRecord ? String(selectedRecord.agent_id ?? selectedRecord.skill_id ?? selectedRecord.automation_id ?? selectedRecord.goal_id ?? "") : "";
  return (
    <section>
      <div className="section-title"><div><p className="eyebrow">CURRENT TENANT</p><h2>{pages.find(([id]) => id === kind)?.[1]}</h2></div><button onClick={load}>重新整理</button></div>
      {error && <p className="error">{error}</p>}
      <div className="split">
        <div className="panel"><h3>目前資源</h3><div className="cards">{rows.map((row, index) => <button className="resource" key={index} onClick={() => setSelected(row)}><strong>{String((row as Json).name ?? (row as Json).goal_id ?? (row as Json).skill_id ?? (row as Json).automation_id ?? "資源")}</strong><span>{String((row as Json).status ?? (row as Json).enabled ?? "")}</span></button>)}</div></div>
        <div className="panel"><h3>詳細資料</h3>{resourceId && <><Presence kind={resourceKind} resourceId={resourceId}/>{aclEnabled ? <AccessControl kind={resourceKind} resourceId={resourceId}/> : <p className="notice">Resource ACL 尚未啟用；目前沿用 tenant／RBAC。</p>}</>}<JsonView value={selected ?? { message: "選擇左側資源" }} /></div>
      </div>
      {config.template && <form className="panel editor" onSubmit={create}><h3>建立／送出 JSON 表單</h3><textarea value={draft} onChange={(event) => setDraft(event.target.value)} spellCheck={false}/><button className="primary">建立</button></form>}
      {kind === "skills" && <SkillImport onResult={setSelected} />}
      {kind === "goals" && <Approval onResult={setSelected} />}
    </section>
  );
}

function SkillImport({ onResult }: { onResult: (value: unknown) => void }) {
  const [repository, setRepository] = useState("");
  const [commit, setCommit] = useState("");
  const [error, setError] = useState("");
  async function submit(event: FormEvent) {
    event.preventDefault();
    try {
      onResult(await api("/v1/skill-imports/github", { method: "POST", body: JSON.stringify({ repository, commit, subdirectory: "" }) }));
    } catch (err) { setError(String(err)); }
  }
  return <form className="panel inline-form" onSubmit={submit}><h3>外部套件匯入（先檢查、後確認）</h3><input value={repository} onChange={(e) => setRepository(e.target.value)} placeholder="owner/repository"/><input value={commit} onChange={(e) => setCommit(e.target.value)} placeholder="固定 40 字元 commit SHA"/><button>建立檢查工作</button>{error && <p className="error">{error}</p>}</form>;
}

function Approval({ onResult }: { onResult: (value: unknown) => void }) {
  const [goal, setGoal] = useState("");
  const [approval, setApproval] = useState("");
  const [approved, setApproved] = useState(true);
  const [error, setError] = useState("");
  async function submit(event: FormEvent) {
    event.preventDefault();
    try {
      onResult(await api(`/v1/goals/${encodeURIComponent(goal)}/approval`, { method: "POST", body: JSON.stringify({ approval_id: approval, approved, reason: "管理台逐次決定" }) }));
    } catch (err) { setError(String(err)); }
  }
  return <form className="panel inline-form" onSubmit={submit}><h3>逐次核准</h3><input value={goal} onChange={(e) => setGoal(e.target.value)} placeholder="Goal ID"/><input value={approval} onChange={(e) => setApproval(e.target.value)} placeholder="Approval ID"/><select value={approved ? "yes" : "no"} onChange={(e) => setApproved(e.target.value === "yes")}><option value="yes">核准</option><option value="no">拒絕</option></select><button>送出決定</button>{error && <p className="error">{error}</p>}</form>;
}

function Dag({ compiled }: { compiled: Json | null }) {
  const nodes = Array.isArray(compiled?.nodes) ? compiled.nodes as Json[] : [];
  const edges = Array.isArray(compiled?.edges) ? compiled.edges as Json[] : [];
  return <div className="dag" aria-label="唯讀工作流 DAG"><p className="eyebrow">READ-ONLY DAG</p>{nodes.map((node, i) => <div className="dag-row" key={String(node.node_id ?? i)}><span>{String(node.node_id ?? `node-${i + 1}`)}</span>{i < nodes.length - 1 && <b>↓</b>}</div>)}{nodes.length === 0 && <p>載入 compiled artifact 後顯示節點。</p>}<small>{edges.length} 條依賴邊；此檢視不提供拖拉修改。</small></div>;
}

function Planning() {
  const [id, setId] = useState("");
  const [session, setSession] = useState<Json | null>(null);
  const [compiled, setCompiled] = useState<Json | null>(null);
  const [yaml, setYaml] = useState("schema_version: 2\nobjective: 分析部署證據\nnodes: []\n");
  const [events, setEvents] = useState<Json[]>([]);
  const [error, setError] = useState("");
  const seen = useRef(new Set<string>());
  const stream = useRef<EventSource | null>(null);
  async function load() {
    setError("");
    try {
      setSession(await api(`/v1/planning-sessions/${encodeURIComponent(id)}`) as Json);
      setCompiled(await api(`/v1/planning-sessions/${encodeURIComponent(id)}/compiled`) as Json);
    } catch (err) { setError(String(err)); }
  }
  function follow() {
    stream.current?.close();
    seen.current.clear();
    const source = new EventSource(`/v1/planning-sessions/${encodeURIComponent(id)}/events`);
    source.onmessage = (event) => {
      if (event.lastEventId && seen.current.has(event.lastEventId)) return;
      if (event.lastEventId) seen.current.add(event.lastEventId);
      try { setEvents((old) => [...old.slice(-99), JSON.parse(event.data)]); } catch { /* ignore invalid server frames */ }
    };
    ["planning.session_created", "plan.revised", "planning.refreshed", "forecast.published", "planning.mode_changed"].forEach((name) => source.addEventListener(name, source.onmessage as EventListener));
    source.onerror = () => setError("事件連線中斷；瀏覽器會以最後事件 ID 自動重連。");
    stream.current = source;
  }
  useEffect(() => () => stream.current?.close(), []);
  async function saveYaml() {
    if (!session) return;
    try {
      setSession(await api(`/v1/planning-sessions/${encodeURIComponent(id)}/workflow-yaml`, { method: "POST", body: JSON.stringify({ expected_version: session.version, yaml }) }) as Json);
    } catch (err) { setError(String(err)); }
  }
  async function forecast() {
    if (!session) return;
    try {
      setCompiled(await api(`/v1/planning-sessions/${encodeURIComponent(id)}/forecasts`, { method: "POST", body: JSON.stringify({ expected_version: session.version, plan_revision: session.plan_revision }) }) as Json);
    } catch (err) { setError(String(err)); }
  }
  return <section><div className="section-title"><div><p className="eyebrow">PLAN MODE ONLY</p><h2>計畫、快照與預測</h2></div></div><div className="inline-form panel"><input value={id} onChange={(e) => setId(e.target.value)} placeholder="Planning session ID"/><button onClick={load}>載入</button><button onClick={follow}>追蹤 SSE</button></div>{error && <p className="error">{error}</p>}<div className="split"><div className="panel editor"><h3>Workflow YAML</h3><textarea value={yaml} onChange={(e) => setYaml(e.target.value)} spellCheck={false}/><div><button onClick={saveYaml}>儲存不可變 revision</button><button onClick={forecast}>建立 forecast</button></div></div><div className="panel"><Dag compiled={(compiled?.compiled as Json) ?? compiled}/></div></div><div className="split"><div className="panel"><h3>Session／Forecast</h3><JsonView value={session ?? compiled}/></div><div className="panel"><h3>安全事件摘要</h3><JsonView value={events}/></div></div></section>;
}

function History() {
  const [query, setQuery] = useState("");
  const [result, setResult] = useState<unknown>({});
  const [error, setError] = useState("");
  async function search(event: FormEvent) {
    event.preventDefault();
    try { setResult(await api(`/v1/history/search?q=${encodeURIComponent(query)}`)); } catch (err) { setError(String(err)); }
  }
  return <section><div className="section-title"><div><p className="eyebrow">CONTROLLED SEARCH</p><h2>歷史搜尋與稽核</h2></div></div><form className="panel inline-form" onSubmit={search}><input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="字面或全文搜尋"/><button>搜尋</button></form>{error && <p className="error">{error}</p>}<div className="panel"><JsonView value={result}/></div></section>;
}

function App() {
  const [session, setSession] = useState<Session | null | undefined>(undefined);
  const [page, setPage] = useState(() => window.location.hash.slice(1) || "agents");
  useEffect(() => { void api("/auth/session").then((value) => setSession((value as Session).authenticated ? value as Session : null)).catch(() => setSession(null)); }, []);
  useEffect(() => { const handler = () => setPage(window.location.hash.slice(1) || "agents"); window.addEventListener("hashchange", handler); return () => window.removeEventListener("hashchange", handler); }, []);
  const content = useMemo(() => page === "planning" ? <Planning/> : page === "history" ? <History/> : <ResourcePage kind={page in resourceConfig ? page : "agents"} aclEnabled={session?.resource_acl_enabled === true}/>, [page, session]);
  if (session === undefined) return <main className="loading">正在確認 session…</main>;
  if (session === null) return <Login/>;
  return <div className="shell"><aside><div><p className="eyebrow">AI OPS</p><h1>Gateway</h1></div><nav>{pages.map(([id, label]) => <a className={page === id ? "active" : ""} href={`#${id}`} key={id}>{label}</a>)}</nav><div className="identity"><strong>{session.subject}</strong><span>{session.tenant_id} · {session.role}</span><form method="post" action="/auth/logout" onSubmit={(event) => { event.preventDefault(); void api("/auth/logout", { method: "POST" }).finally(() => window.location.reload()); }}><button>登出</button></form></div></aside><main>{content}</main></div>;
}

export default App;
