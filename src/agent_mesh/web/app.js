const language = {
  nodes: ["Nodes", "节点"],
  tasks: ["Tasks", "任务"],
  mainNavigation: ["Main navigation", "主导航"],
  nodeDirectory: ["Discovered nodes", "已发现的节点"],
  noNodes: ["No nodes have been discovered yet.", "尚未发现节点。"],
  selectNode: ["Select a node", "选择节点"],
  nodeHint: ["Select a node to view its capabilities.", "选择节点查看它提供的能力。"],
  nodeMissing: ["The selected node has not been discovered.", "当前未发现所选节点。"],
  chooseNode: ["Choose a node from the directory.", "可从目录中选择节点。"],
  localNode: ["Local", "本机"],
  nodeOnline: ["Online", "在线"],
  nodeOffline: ["Offline", "离线"],
  nodeUnknown: ["Unknown", "未知"],
  capabilities: ["Capabilities", "能力"],
  noCapabilities: ["This node does not advertise any capabilities.", "此节点未公开能力。"],
  singleTurn: ["Single turn", "单轮"],
  multiTurn: ["Multi-turn", "可续接"],
  clarify: ["Can ask questions", "支持澄清"],
  call: ["Call", "调用"],
  refresh: ["Refresh", "刷新"],
  newTask: ["New task", "发起任务"],
  languageName: ["简体中文", "English"],
  languageLabel: ["Switch to Simplified Chinese", "切换到英文"],
  taskList: ["Task list", "任务列表"],
  taskFilters: ["Task filters", "任务筛选"],
  recent: ["Recent", "近期任务"],
  recentScope: ["Latest 100 rounds", "最近 100 轮"],
  waiting: ["Needs reply", "待回答"],
  pending: ["Reply unconfirmed", "答复待确认"],
  answered: ["Answered", "已答复"],
  expired: ["Expired", "已过期"],
  accepted: ["In progress", "执行中"],
  succeeded: ["Completed", "已完成"],
  failed: ["Failed", "失败"],
  rejected: ["Rejected", "未接收"],
  unconfirmed: ["Unconfirmed", "待确认"],
  unknown: ["Outcome unknown", "结果未知"],
  offline: ["Agent offline", "节点离线"],
  online: ["Agent online", "节点在线"],
  unknownConnection: ["Connection unknown", "连通性未知"],
  connectionLoading: ["Connecting locally", "正在连接本地服务"],
  localAvailable: ["Local service available", "本地服务可用"],
  localUnavailable: ["Local service unavailable", "本地服务不可用"],
  daemonUnavailable: ["Daemon unavailable", "Daemon 不可用"],
  brokerOnline: ["Broker connected", "Broker 已连接"],
  brokerOffline: ["Broker disconnected", "Broker 已断开"],
  accessTitle: ["Open the local access link", "使用本地访问链接"],
  accessMessage: ["Open a fresh link from the CLI to view your mesh.", "请使用 CLI 生成的新链接打开工作台。"],
  noJobs: ["No tasks yet. Start a task or check back after your next CLI or MCP call.", "暂无任务。可发起任务，CLI 或 MCP 的调用也会显示在这里。"],
  noWaiting: ["No unanswered questions in the latest 100 rounds.", "最近 100 轮没有待回答的问题。"],
  noSelection: ["Select a task", "选择任务"],
  taskUnavailable: ["Task unavailable", "任务不可用"],
  detailHint: ["Select a task to see its progress and result.", "选择一轮任务，查看进展和结果。"],
  loading: ["Loading…", "正在加载…"],
  refreshError: ["Could not refresh the task list.", "无法刷新任务列表。"],
  loadError: ["Could not load task details.", "无法读取任务详情。"],
  targetError: ["Could not refresh agent discovery.", "无法刷新节点发现信息。"],
  connectionError: ["Could not reach the local service.", "无法连接本地服务。"],
  responseError: ["The local service returned an unreadable response.", "本地服务返回了无法读取的响应。"],
  deadline: ["Reply by", "答复截止"],
  wasDue: ["Reply window closed", "答复已截止"],
  jobId: ["Job ID", "任务 ID"],
  sessionId: ["Session ID", "会话 ID"],
  taskInfo: ["Task info", "任务信息"],
  originalRound: ["Original round", "原始轮次"],
  latestRound: ["Latest reply round", "最新答复轮"],
  round: ["Reply round", "答复轮"],
  roundRequest: ["Request", "本轮请求"],
  roundReplyText: ["Reply text", "本轮答复"],
  waitingReceipt: ["Waiting for confirmation from the agent.", "正在等待目标节点确认。"],
  noOutput: ["The agent returned no output.", "节点未返回输出。"],
  previousRound: ["Open the previous question", "查看上一轮问题"],
  viewRound: ["Open round", "查看轮次"],
  continueSession: ["Continue session", "继续会话"],
  replyLabel: ["Your reply", "答复"],
  replyPlaceholder: ["Enter your answer to the question above", "输入对上方问题的答复"],
  sendReply: ["Send reply", "发送答复"],
  replySent: ["Reply submitted.", "答复已提交。"],
  requireReply: ["Enter a reply.", "请输入答复。"],
  cannotSubmit: ["Wait for the connection before submitting.", "恢复连接后可提交。"],
  sending: ["Submitting…", "正在提交…"],
  target: ["Agent", "目标节点"],
  method: ["Method", "方法"],
  chooseTarget: ["Select an agent", "选择节点"],
  chooseMethod: ["Select a method", "选择方法"],
  noAgents: ["No agents with available methods have been discovered.", "尚未发现公开方法的节点。"],
  targetUnavailable: ["The selected agent is unavailable.", "所选节点当前不可用。"],
  methodUnavailable: ["The selected method is unavailable.", "所选方法当前不可用。"],
  inputSchema: ["Input schema", "输入约定"],
  format: ["Input format", "输入格式"],
  prompt: ["Prompt", "提示词"],
  jsonParams: ["JSON parameters", "JSON 参数"],
  promptPlaceholder: ["Enter the request and any relevant constraints", "输入需求和相关约束"],
  requirePrompt: ["Enter a prompt.", "请输入提示词。"],
  requireObject: ["Enter a valid JSON object.", "请输入有效的 JSON 对象。"],
  submitTask: ["Submit task", "提交任务"],
  taskSent: ["Task submitted.", "任务已提交。"],
  close: ["Close", "关闭"],
  cancel: ["Cancel", "取消"],
  requestDenied: ["Submission error", "提交错误"],
  queryError: ["Query error", "查询错误"],
  taskError: ["Execution error", "执行错误"],
};

const storageKey = "agent-mesh.workspace";
const tokenKey = "agent-mesh.access-token";
const localLimit = 100;
const pollInterval = 5000;
const byId = (id) => document.getElementById(id);

function read(key) {
  try { return sessionStorage.getItem(key); }
  catch { return null; }
}

function write(key, value) {
  try {
    if (value === null) sessionStorage.removeItem(key);
    else sessionStorage.setItem(key, value);
  } catch { /* The current page still works when storage is unavailable. */ }
}

function savedWorkspace() {
  try { return JSON.parse(read(storageKey)) || {}; }
  catch { return {}; }
}

function openToken() {
  const fragment = new URLSearchParams(location.hash.slice(1));
  if (fragment.has("token")) {
    const token = fragment.get("token");
    history.replaceState(history.state, "", location.pathname + location.search);
    write(tokenKey, token || null);
    return { token, fresh: true };
  }
  return { token: read(tokenKey), fresh: false };
}

const saved = savedWorkspace();
const access = openToken();
const state = {
  token: access.token,
  locale: saved.locale ?? (navigator.languages?.[0] || navigator.language || "").toLowerCase().startsWith("zh"),
  view: !access.fresh && saved.view === "tasks" ? "tasks" : "nodes",
  nodeId: saved.nodeId || null,
  selected: saved.selected || null,
  filter: saved.filter === "waiting" ? "waiting" : "recent",
  drafts: saved.drafts || {},
  composerDrafts: saved.composerDrafts || {},
  composeKey: access.fresh ? null : saved.composeKey || null,
  jobs: new Map(),
  agents: [],
  localNode: null,
  brokerConnected: null,
  refreshed: false,
  agentReady: false,
  listError: null,
  agentError: null,
  detailError: null,
  detailData: null,
  detailRequest: null,
  refreshRequest: null,
  currentReply: null,
  replyErrors: new Map(),
  sentAnswers: new Map(),
  submittingReply: null,
  submittingTask: false,
};

function t(key) {
  return (language[key] || [key, key])[state.locale ? 1 : 0];
}

function persist() {
  write(storageKey, JSON.stringify({
    locale: state.locale,
    view: state.view,
    nodeId: state.nodeId,
    selected: state.selected,
    filter: state.filter,
    drafts: state.drafts,
    composerDrafts: state.composerDrafts,
    composeKey: state.composeKey,
  }));
}

function applyLanguage() {
  document.documentElement.lang = state.locale ? "zh-CN" : "en";
  for (const n of document.querySelectorAll("[data-text]")) n.textContent = t(n.dataset.text);
  for (const n of document.querySelectorAll("[data-label]")) n.setAttribute("aria-label", t(n.dataset.label));
  for (const n of document.querySelectorAll("[data-placeholder]")) n.placeholder = t(n.dataset.placeholder);
  const switcher = byId("language");
  switcher.textContent = t("languageName");
  switcher.lang = state.locale ? "en" : "zh-CN";
  switcher.title = t("languageLabel");
}

function node(tag, className, ...children) {
  const n = document.createElement(tag);
  if (className) n.className = className;
  n.append(...children);
  return n;
}

function button(text, className, action) {
  const n = node("button", className, text);
  n.type = "button";
  n.addEventListener("click", action);
  return n;
}

function setMessage(id, message) {
  const n = byId(id);
  if (n.textContent !== (message || "")) n.textContent = message || "";
  n.hidden = !message;
}

function announce(text) {
  byId("announcements").textContent = text;
}

class ApiError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}

function lock() {
  state.token = null;
  write(tokenKey, null);
  if (byId("compose").open) byId("compose").close();
  render();
}

async function api(action, args = {}, signal) {
  let response;
  try {
    response = await fetch(`/api/${action}`, {
      method: "POST",
      credentials: "omit",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${state.token}`,
      },
      body: JSON.stringify(args),
      signal,
    });
  } catch (error) {
    if (error.name === "AbortError") throw error;
    throw new ApiError("NETWORK", t("connectionError"));
  }
  if (response.status === 401 || response.status === 403) {
    lock();
    throw new ApiError("ACCESS", t("accessMessage"));
  }
  let data;
  try { data = await response.json(); }
  catch { throw new ApiError("RESPONSE", t("responseError")); }
  if (!response.ok) {
    throw new ApiError(data.error?.code || String(response.status), data.error?.message || response.statusText);
  }
  return data;
}

function problem(error) {
  return error?.code ? `${error.code} · ${error.message}` : error?.message || "";
}

function errorMessage(error) {
  return error?.code === "NETWORK" ? t("connectionError") : problem(error);
}

function stateOf(job) {
  return job?.snapshot?.status || job?.status || "unconfirmed";
}

function info(job) {
  return job?.snapshot || job;
}

function questionState(job) {
  if (stateOf(job) !== "needs_input") return null;
  const status = job.answer?.status || "waiting";
  if (status !== "waiting") return status;
  if (state.sentAnswers.has(job.job_id)) return "pending";
  const deadline = job.answer?.expires_at || info(job).question?.expires_at;
  return deadline && deadline * 1000 <= Date.now() ? "expired" : "waiting";
}

function displayState(job) {
  if (stateOf(job) === "unconfirmed" && job.invoke_error) {
    return job.invoke_error.code === "OUTCOME_UNKNOWN" ? "unknown" : "rejected";
  }
  return questionState(job) || stateOf(job);
}

function statusBadge(status) {
  return node("span", `status ${status}`, t(status));
}

function resultText(result) {
  if (typeof result === "string") return result;
  if (result && typeof result.text === "string" && Object.keys(result).length === 1) return result.text;
  return result == null ? "" : JSON.stringify(result, null, 2);
}

function requestPreview(job) {
  return typeof job.request_preview === "string" ? job.request_preview.trim() : "";
}

function preview(job) {
  const snapshot = info(job);
  const text = snapshot.question?.text || (
    typeof snapshot.result?.text === "string" ? snapshot.result.text : resultText(snapshot.result)
  );
  return (text || snapshot.error?.message || job.invoke_error?.message || "").trim().split(/\r?\n/, 1)[0].slice(0, 220);
}

function formatTime(seconds, short = false) {
  if (typeof seconds !== "number" || !Number.isFinite(seconds)) return "";
  const date = new Date(seconds * 1000);
  if (Number.isNaN(date.getTime())) {
    return state.locale ? `Unix 时间 ${seconds} 秒` : `Unix time ${seconds} s`;
  }
  const locale = state.locale ? "zh-CN" : "en";
  return new Intl.DateTimeFormat(locale, short
    ? { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }
    : { dateStyle: "medium", timeStyle: "short" }).format(date);
}

function agent(id) {
  return state.agents.find((candidate) => candidate.id === id);
}

function agentLabel(id) {
  return agent(id)?.name || id || "";
}

function agentStatus(id) {
  if (state.agentError || state.brokerConnected === false) return "unknown";
  return agent(id)?.status || "unknown";
}

function brokerOffline() {
  return state.brokerConnected !== null ? !state.brokerConnected
    : state.agentReady && state.agents.length > 0 && state.agents.every((item) => item.status === "unknown");
}

function localIssue() {
  const errors = [state.agentError?.code, state.view === "tasks" ? state.listError?.code : null];
  if (errors.includes("DAEMON_UNAVAILABLE")) return "daemonUnavailable";
  if (errors.includes("NETWORK")) return "localUnavailable";
  return null;
}

function connectionUnavailable() {
  return !state.token || !!localIssue() || brokerOffline();
}

function renderHeader() {
  const issue = localIssue();
  let status = "connectionLoading";
  let color = "";
  if (!state.token) status = "accessTitle";
  else if (issue) {
    status = issue;
    color = "bad";
  } else if (brokerOffline()) {
    status = "brokerOffline";
    color = "warning";
  } else if (state.brokerConnected === true
      || (state.agentReady && state.agents.some((item) => item.status === "online" || item.status === "offline"))) {
    status = "brokerOnline";
    color = "good";
  } else if (state.refreshed || state.agentReady) {
    status = "localAvailable";
  }
  byId("connection").className = `connection ${color}`;
  byId("connection-label").textContent = t(status);
  byId("refresh").disabled = !state.token || !!state.refreshRequest;
  byId("new-task").disabled = !state.token;
  byId("new-task").hidden = !state.token || state.view !== "tasks";
  byId("access").hidden = !!state.token;
  byId("main-navigation").hidden = !state.token;
  byId("nodes-pane").hidden = !state.token || state.view !== "nodes";
  byId("workspace").hidden = !state.token || state.view !== "tasks";
  const title = t(state.view);
  byId("page-title").textContent = title;
  document.title = `${title} · Agent Mesh`;
  for (const view of ["nodes", "tasks"]) {
    const tab = byId(`${view}-nav`);
    if (view === state.view) tab.setAttribute("aria-current", "page");
    else tab.removeAttribute("aria-current");
  }
}

function capabilityCount(count) {
  return state.locale ? `${count} 项能力` : `${count} ${count === 1 ? "capability" : "capabilities"}`;
}

function nodeStatus(status) {
  const value = status === "online" ? "Online" : status === "offline" ? "Offline" : "Unknown";
  return node("span", `status node-${value.toLowerCase()}`, t(`node${value}`));
}

function nodeRows() {
  return state.agents.map((item) => ({
    id: item.id,
    label: item.name || item.id,
    status: agentStatus(item.id),
    local: item.id === state.localNode,
    methods: capabilities(item.id).map((capability) => capability.method),
  })).sort((first, second) => {
    const priority = (row) => (row.status === "online" ? 2 : 0) + (row.methods.length ? 1 : 0);
    return priority(second) - priority(first);
  });
}

let nodeListKey = "";
function renderNodeList(rows) {
  setMessage("nodes-error", state.agentError ? `${t("targetError")} ${errorMessage(state.agentError)}` : "");
  const isMissingLocalCard = state.localNode && !agent(state.localNode);
  const emptyMessage = !state.agentReady ? "loading" : "noNodes";
  const localIdentity = isMissingLocalCard ? `${t("localNode")} · ${state.localNode}` : "";
  setMessage("local-node-note", localIdentity);
  setMessage("nodes-empty", rows.length || state.agentError ? "" : t(emptyMessage));

  const key = JSON.stringify([rows, state.nodeId, state.locale]);
  if (key === nodeListKey) return;
  nodeListKey = key;
  const list = byId("node-list");
  const focused = document.activeElement?.dataset.node;
  list.replaceChildren(...rows.map((item) => {
    const row = button("", "task-row node-row", () => selectNode(item.id, { scroll: true }));
    row.dataset.node = item.id;
    if (item.id === state.nodeId) row.setAttribute("aria-current", "true");
    const name = node("span", "node-name-line", node("strong", "node-name", item.label));
    if (item.local) name.append(node("span", "local-mark", t("localNode")));
    const meta = [item.label !== item.id ? item.id : "", capabilityCount(item.methods.length)].filter(Boolean).join(" · ");
    row.append(name, nodeStatus(item.status), node("span", "task-meta", meta));
    if (item.methods.length) {
      row.append(node("span", "task-preview node-method-preview", item.methods.slice(0, 3).join(" · ")));
    }
    return node("li", "", row);
  }));
  if (focused) [...list.querySelectorAll("button")].find((item) => item.dataset.node === focused)?.focus({ preventScroll: true });
}

let nodeDetailKey = "";
function renderNodeDetail(selected) {
  const host = byId("node-content");
  const methods = selected ? capabilities(selected.id) : [];
  const blocked = connectionUnavailable();
  const key = JSON.stringify([selected, methods, blocked, state.locale, state.nodeId]);
  if (key === nodeDetailKey) return;
  nodeDetailKey = key;
  if (!selected) {
    const heading = node("h2", "", t(state.nodeId ? "nodeMissing" : "selectNode"));
    heading.id = "node-title";
    host.replaceChildren(node("div", "empty", heading, node("p", "", t(state.nodeId ? "chooseNode" : "nodeHint"))));
    return;
  }

  const heading = node("h2", "node-detail-title", selected.label);
  heading.id = "node-title";
  const header = node("div", "detail-header", heading, nodeStatus(selected.status));
  const meta = [selected.label !== selected.id ? selected.id : "", selected.local ? t("localNode") : ""].filter(Boolean);
  const views = [header];
  if (meta.length) views.push(node("p", "detail-meta", meta.join(" · ")));
  const capabilitiesHeader = node("div", "capability-header",
    node("h3", "", t("capabilities")),
    node("span", "capability-count", String(methods.length)));
  views.push(capabilitiesHeader);
  if (!methods.length) {
    views.push(node("p", "capability-empty", t("noCapabilities")));
  } else {
    if (blocked) views.push(node("p", "capability-empty", t("cannotSubmit")));
    views.push(node("ul", "capability-list", ...methods.map((capability) => {
      const action = button(t("call"), "button primary capability-call", (event) =>
        openComposer({ target: selected.id, method: capability.method, opener: event.currentTarget }));
      action.setAttribute("aria-label", `${t("call")} ${capability.method}`);
      action.disabled = blocked;
      const interaction = capability.interaction === "clarify" ? "clarify"
        : capability.interaction === "session" ? "multiTurn" : "singleTurn";
      const title = node("h4", "capability-method", capability.method);
      const body = node("div", "capability-body", title, node("p", "capability-behavior", t(interaction)));
      if (capability.description) body.append(node("p", "capability-text", capability.description));
      return node("li", "capability-item", body, action);
    })));
  }
  host.replaceChildren(...views);
}

function renderNodes() {
  const rows = nodeRows();
  if (!state.nodeId && rows.length) {
    state.nodeId = rows[0].id;
    persist();
  }
  renderNodeList(rows);
  renderNodeDetail(rows.find((row) => row.id === state.nodeId));
}

function selectNode(id, { scroll = false } = {}) {
  if (id !== state.nodeId) {
    state.nodeId = id;
    persist();
    renderNodes();
  }
  if (scroll && matchMedia("(max-width: 675px)").matches) byId("node-panel").scrollIntoView({ block: "start" });
}

let queueKey = "";
function renderQueue() {
  const all = [...state.jobs.values()];
  const waiting = all.filter((item) => questionState(item) === "waiting");
  const rows = (state.filter === "waiting" ? waiting : all).map((job) => {
    const request = requestPreview(job);
    const meta = [agentLabel(job.target_id), formatTime(job.submitted_at, true)];
    if (request) meta.unshift(job.method);
    return {
      id: job.job_id,
      title: request || job.method || t("method"),
      request: !!request,
      meta: meta.filter(Boolean).join(" · "),
      label: displayState(job),
      preview: preview(job),
    };
  });
  byId("recent-filter").setAttribute("aria-pressed", String(state.filter === "recent"));
  byId("waiting-filter").setAttribute("aria-pressed", String(state.filter === "waiting"));
  byId("waiting-count").textContent = String(waiting.length);
  byId("waiting-count").hidden = waiting.length === 0;
  const empty = !state.refreshed ? "loading" : state.filter === "waiting" ? "noWaiting" : "noJobs";
  setMessage("queue-empty", rows.length || state.listError ? "" : t(empty));
  setMessage("queue-error", state.listError ? `${t("refreshError")} ${errorMessage(state.listError)}` : "");

  const key = JSON.stringify([rows, state.selected, state.locale]);
  if (key === queueKey) return;
  queueKey = key;
  const focused = document.activeElement?.dataset.job;
  const makeRow = (item) => {
    const row = button("", "task-row", () => selectJob(item.id, { scroll: true }));
    row.dataset.job = item.id;
    if (item.id === state.selected) row.setAttribute("aria-current", "true");
    row.append(
      node("strong", item.request ? "task-title" : "task-method", item.title),
      statusBadge(item.label),
      node("span", "task-meta", item.meta),
    );
    if (item.preview) row.append(node("span", "task-preview", item.preview));
    return node("li", "", row);
  };
  const n = byId("task-list");
  n.replaceChildren(...rows.map(makeRow));
  if (focused) [...n.querySelectorAll("button")].find((item) => item.dataset.job === focused)?.focus({ preventScroll: true });
}

function readJob(id) {
  const detail = state.detailData;
  const detailed = detail?.job_id === id ? detail
    : detail?.latest_answer?.job_id === id ? detail.latest_answer : null;
  const summary = state.jobs.get(id);
  let job = summary || detailed;
  if (detailed && summary) {
    job = { ...detailed, ...summary, latest_answer: detailed.latest_answer };
    if (["unconfirmed", "accepted"].includes(stateOf(summary)) && detailed.snapshot
        && (stateOf(detailed) !== "accepted" || !summary.snapshot)) job.snapshot = detailed.snapshot;
    if (detailed.answer?.status === "answered" && summary.answer?.status !== "answered") {
      job.answer = detailed.answer;
    }
  }
  const attempt = state.jobs.get(state.sentAnswers.get(id));
  if (job?.answer?.status === "waiting" && attempt?.snapshot) {
    return { ...job, answer: { ...job.answer, status: "answered", job_id: attempt.job_id } };
  }
  return job;
}

function chain(root) {
  const rounds = [root];
  const visited = new Set([root.job_id]);
  let current = root;
  while (current.answer?.status === "answered") {
    const next = readJob(current.answer.job_id) || root.latest_answer;
    if (!next || visited.has(next.job_id)) break;
    rounds.push(next);
    visited.add(next.job_id);
    current = next;
  }
  return rounds;
}

function idLink(id, text) {
  return button(text || id, text ? "text-button" : "text-button linked-id", () => selectJob(id));
}

function information(rounds, open) {
  const task = rounds[0];
  const infoPanel = node("details", "task-info", node("summary", "", t("taskInfo")));
  infoPanel.dataset.job = task.job_id;
  infoPanel.open = open;
  const list = node("dl", "ids", node("dt", "", t("jobId")), node("dd", "", task.job_id));
  const session = task.session_id || info(task).session_id;
  if (session) list.append(node("dt", "", t("sessionId")), node("dd", "", session));
  for (const reply of rounds.slice(1)) {
    list.append(node("dt", "", t("round")), node("dd", "", idLink(reply.job_id)));
  }
  infoPanel.append(list);
  return infoPanel;
}

function addProblem(parent, label, error, appearance = "error") {
  if (error) parent.append(node("p", `notice ${appearance}`, `${t(label)} · ${problem(error)}`));
}

function turn(job, title) {
  const snapshot = info(job);
  const view = node("article", "turn");
  if (title) {
    view.append(node("header", "turn-heading", node("h3", "", t(title)), statusBadge(displayState(job))));
  }
  const request = requestPreview(job);
  if (request) {
    view.append(node("div", "request-preview",
      node("p", "request-title", t(job.answer_to ? "roundReplyText" : "roundRequest")),
      node("p", "request-text", request)));
  }
  if (snapshot.question?.text) {
    view.append(node("p", "output question", snapshot.question.text));
    const deadline = snapshot.question.expires_at;
    if (deadline) view.append(node("p", "turn-caption", `${t(questionState(job) === "expired" ? "wasDue" : "deadline")} · ${formatTime(deadline)}`));
    if (questionState(job) === "pending" || questionState(job) === "answered") {
      const pendingIds = job.answer?.pending_job_ids || [job.answer?.job_id || state.sentAnswers.get(job.job_id)].filter(Boolean);
      if (pendingIds.length) {
        const note = node("div", "notice warning", t(questionState(job)),
          node("div", "notice-links", ...pendingIds.map((id) => idLink(id))));
        view.append(note);
      }
    }
  } else if (snapshot.result != null) {
    const text = resultText(snapshot.result);
    const formatted = typeof snapshot.result === "object" && text !== snapshot.result.text;
    view.append(node("pre", formatted ? "output mono" : "output", text || t("noOutput")));
  } else if (stateOf(job) === "unconfirmed" && !job.invoke_error) {
    view.append(node("p", "turn-caption", t("waitingReceipt")));
  }
  addProblem(view, "taskError", snapshot.error);
  addProblem(view, "requestDenied", job.invoke_error);
  addProblem(view, "queryError", job.query_error, "warning");
  return view;
}

function renderReply(activeJob) {
  const id = activeJob && questionState(activeJob) === "waiting" ? activeJob.job_id : null;
  const input = byId("reply-input");
  const currentlySending = id !== null && state.submittingReply === id;
  const isBlocked = id !== null && connectionUnavailable();
  byId("reply-form").hidden = !id;
  if (state.currentReply !== id) {
    state.currentReply = id;
    input.value = id ? state.drafts[id] || "" : "";
  }
  if (!id) return;
  input.disabled = currentlySending;
  byId("reply-submit").disabled = !!state.submittingReply || isBlocked;
  byId("reply-submit").textContent = t(currentlySending ? "sending" : "sendReply");
  const problem = state.replyErrors.get(id);
  setMessage("reply-message", problem ? errorMessage(problem) : isBlocked ? t("cannotSubmit") : "");
}

let detailKey = "";
function renderDetails() {
  const root = readJob(state.selected);
  const rounds = root ? chain(root) : [];
  const activeJob = rounds[rounds.length - 1];
  const statuses = rounds.map(displayState);
  const targetStatus = root ? agentStatus(root.target_id) : null;
  const key = JSON.stringify([state.locale, state.selected, rounds, statuses, targetStatus, state.detailError?.message, !!state.detailRequest]);
  renderReply(activeJob);
  if (key === detailKey) return;
  detailKey = key;
  const host = byId("detail-content");
  if (!root) {
    const title = state.detailError ? "taskUnavailable" : state.selected ? "loading" : "noSelection";
    const heading = node("h2", "", t(title));
    heading.id = "detail-title";
    const note = state.detailError ? errorMessage(state.detailError) : t("detailHint");
    host.replaceChildren(node("div", "empty", heading, node("p", "", note)));
    return;
  }
  const heading = node("h2", "detail-title", root.method);
  heading.id = "detail-title";
  const metadata = [agentLabel(root.target_id), formatTime(root.submitted_at)].filter(Boolean);
  if (targetStatus !== "online") metadata.push(t(targetStatus === "offline" ? "offline" : "unknownConnection"));
  const actions = node("div", "detail-actions");
  if (root.answer_to) actions.append(idLink(root.answer_to, t("previousRound")));
  const sessionId = activeJob.session_id || info(activeJob).session_id;
  if (sessionId && ["succeeded", "failed", "needs_input"].includes(stateOf(activeJob))
      && (!questionState(activeJob) || questionState(activeJob) === "expired")
      && (info(activeJob).error?.code !== "SESSION_NOT_FOUND")) {
    actions.append(button(t("continueSession"), "text-button", (event) => openComposer({
      target: activeJob.target_id, method: activeJob.method, session: sessionId, opener: event.currentTarget,
    })));
  }
  const view = [
    node("div", "detail-header", heading, statusBadge(displayState(activeJob))),
    node("p", "detail-meta", metadata.join(" · ")),
  ];
  if (state.detailError) view.push(node("p", "notice warning", `${t("loadError")} ${errorMessage(state.detailError)}`));
  for (const [index, job] of rounds.entries()) {
    const title = rounds.length === 1 ? "" : index === 0 ? "originalRound"
      : index === rounds.length - 1 ? "latestRound" : "round";
    view.push(turn(job, title));
  }
  if (actions.childElementCount) view.push(actions);
  const previousInfo = host.querySelector(".task-info");
  view.push(information(rounds, previousInfo?.dataset.job === root.job_id && previousInfo.open));
  host.replaceChildren(...view);
}

function render() {
  renderHeader();
  if (!state.token) return;
  if (state.view === "nodes") renderNodes();
  else {
    renderQueue();
    renderDetails();
  }
  if (byId("compose").open) renderComposer();
}

function switchView(view, { load = true } = {}) {
  if (state.view === view) return;
  state.view = view;
  if (view === "nodes") {
    state.detailRequest?.abort();
    state.detailRequest = null;
  }
  persist();
  render();
  if (load) void refreshLists().then(() => {
    if (state.view === "tasks" && !state.detailRequest && state.detailData?.job_id !== state.selected) {
      void refreshDetail();
    }
  });
}

function reconcileAnswers() {
  for (const [id, attemptId] of state.sentAnswers) {
    const job = state.jobs.get(id);
    const attempt = state.jobs.get(attemptId);
    if (job?.answer?.status === "answered" || job?.answer?.status === "pending"
        || (attempt?.invoke_error && attempt.invoke_error.code !== "OUTCOME_UNKNOWN")) {
      state.sentAnswers.delete(id);
    }
  }
  let changed = false;
  for (const id of Object.keys(state.drafts)) {
    if (readJob(id)?.answer?.status === "answered") {
      delete state.drafts[id];
      changed = true;
    }
  }
  if (changed) persist();
}

function refreshLists() {
  if (!state.token) return Promise.resolve();
  const view = state.view;
  if (state.refreshRequest) {
    const current = state.refreshRequest;
    return current.view === view ? current.promise : current.promise.then(refreshLists);
  }
  const jobsRequest = view === "tasks" ? api("jobs", { limit: localLimit }) : null;
  const promise = Promise.allSettled([jobsRequest, api("list")])
    .then(([jobs, agents]) => {
      if (!state.token) return;
      if (jobsRequest) {
        if (jobs.status === "fulfilled") {
          state.jobs = new Map(jobs.value.jobs.map((job) => [job.job_id, job]));
          state.listError = null;
          state.refreshed = true;
          reconcileAnswers();
          if (!state.selected && state.jobs.size) selectJob(state.jobs.keys().next().value, { query: false });
        } else {
          state.listError = jobs.reason;
        }
      }
      if (agents.status === "fulfilled") {
        state.agents = agents.value.agents;
        state.localNode = agents.value.local_id || null;
        state.brokerConnected = typeof agents.value.broker_connected === "boolean"
          ? agents.value.broker_connected : null;
        state.agentReady = true;
        state.agentError = null;
      } else {
        state.brokerConnected = null;
        state.agentReady = false;
        state.agentError = agents.reason;
      }
    }).finally(() => {
      state.refreshRequest = null;
      render();
    });
  state.refreshRequest = { view, promise };
  renderHeader();
  return promise;
}

async function refreshDetail() {
  const id = state.selected;
  if (!id || !state.token || state.view !== "tasks") return;
  state.detailRequest?.abort();
  const request = new AbortController();
  state.detailRequest = request;
  state.detailError = null;
  renderDetails();
  try {
    const job = await api("job_status", { job_id: id }, request.signal);
    if (state.detailRequest === request) state.detailData = job;
  } catch (error) {
    if (state.detailRequest === request && error.name !== "AbortError") state.detailError = error;
  } finally {
    if (state.detailRequest === request) {
      state.detailRequest = null;
      render();
    }
  }
}

function selectJob(id, { query = true, scroll = false, detail = null } = {}) {
  if (id !== state.selected) {
    state.detailRequest?.abort();
    state.detailRequest = null;
    state.detailData = detail;
    state.detailError = null;
    state.selected = id;
    persist();
    render();
    if (query) void refreshDetail();
  }
  if (scroll && matchMedia("(max-width: 675px)").matches) byId("detail-panel").scrollIntoView({ block: "start" });
}

function composer() {
  return state.composerDrafts[state.composeKey] || {};
}

let composerFocus = null;
function openComposer({ target, method, session, opener } = {}) {
  if (!state.token) return;
  composerFocus = opener || null;
  state.composeKey = session || (target && method ? JSON.stringify([target, method]) : "new");
  state.composerDrafts[state.composeKey] ||= { format: "prompt", prompt: "", json: "" };
  if (target && method) Object.assign(composer(), { target, method, session, pinned: true });
  persist();
  showComposer();
}

function showComposer() {
  if (!state.composeKey || !state.token) return;
  const draft = composer();
  byId("prompt-input").value = draft.prompt || "";
  byId("json-input").value = draft.json || "";
  byId("capability-schema").open = false;
  setMessage("compose-message", "");
  renderComposer();
  if (!byId("compose").open) byId("compose").showModal();
}

function restoreComposerFocus() {
  const origin = composerFocus;
  composerFocus = null;
  if (!state.token) return;
  if (origin?.isConnected && !origin.disabled && !origin.closest("[hidden]")) {
    origin.focus();
    return;
  }
  const fallback = state.view === "nodes"
    ? byId("node-list").querySelector('[aria-current="true"]')
    : byId(state.selected ? "detail-panel" : "new-task");
  (fallback || byId(`${state.view}-nav`)).focus();
}

function options(id, values, selected) {
  const select = byId(id);
  const signature = JSON.stringify(values);
  if (select.dataset.options !== signature) {
    select.dataset.options = signature;
    select.replaceChildren(...values.map(([value, text]) => new Option(text, value)));
  }
  select.value = selected;
}

function capabilities(target) {
  return (agent(target)?.capabilities || []).filter((item) => typeof item?.method === "string");
}

function renderComposer() {
  const draft = composer();
  const activeAgents = state.agents.filter((item) => capabilities(item.id).length);
  if (!draft.target && activeAgents.length) {
    draft.target = (activeAgents.find((item) => item.status === "online") || activeAgents[0]).id;
    persist();
  }
  const targets = activeAgents.map((item) => [item.id, item.name && item.name !== item.id ? `${item.name} · ${item.id}` : item.id]);
  if (draft.target && !targets.some(([id]) => id === draft.target)) targets.push([draft.target, draft.target]);
  options("target", [["", t("chooseTarget")], ...targets], draft.target || "");
  const methods = capabilities(draft.target);
  if (!draft.method && methods.length) {
    draft.method = methods[0].method;
    persist();
  }
  const choices = methods.map((item) => [item.method, item.method]);
  if (draft.method && !choices.some(([name]) => name === draft.method)) choices.push([draft.method, draft.method]);
  options("method", [["", t("chooseMethod")], ...choices], draft.method || "");
  const pinned = !!draft.pinned || !!draft.session;
  byId("compose-context").hidden = !pinned;
  byId("compose-selects").hidden = pinned;
  if (pinned) {
    const label = agentLabel(draft.target);
    byId("compose-node").textContent = label !== draft.target ? `${label} (${draft.target})` : label;
    byId("compose-method").textContent = draft.method;
  }
  for (const id of ["target", "method"]) {
    const select = byId(id);
    select.disabled = pinned || state.submittingTask;
    select.required = !pinned;
  }
  byId("compose-title").textContent = t(draft.session ? "continueSession" : "newTask");
  setMessage("compose-session", draft.session ? `${t("sessionId")} · ${draft.session}` : "");
  const capability = methods.find((item) => item.method === draft.method);
  setMessage("capability-description", capability?.description);
  byId("capability-schema").hidden = !capability?.input_schema;
  const schemaText = capability?.input_schema ? JSON.stringify(capability.input_schema, null, 2) : "";
  if (byId("schema-text").textContent !== schemaText) byId("schema-text").textContent = schemaText;
  const availability = agentStatus(draft.target);
  let targetMessage = t(availability === "online" ? "online" : availability === "offline" ? "offline" : "unknownConnection");
  if (draft.target && !agent(draft.target)) targetMessage = t("targetUnavailable");
  if (!activeAgents.length && !draft.target) targetMessage = t("noAgents");
  else if (!capability && availability === "online") targetMessage = t("methodUnavailable");
  if (state.agentError) targetMessage = `${t("targetError")} ${errorMessage(state.agentError)}`;
  if (!state.agentReady && !state.agentError) targetMessage = t("loading");
  byId("target-status").textContent = targetMessage;
  byId("format").value = draft.format || "prompt";
  byId("prompt-field").hidden = draft.format === "json";
  byId("json-field").hidden = draft.format !== "json";
  byId("prompt-input").required = draft.format !== "json";
  byId("json-input").required = draft.format === "json";
  byId("compose-submit").disabled = state.submittingTask || !capability || connectionUnavailable();
  byId("compose-submit").textContent = t(state.submittingTask ? "sending" : "submitTask");
}

async function submitTask(event) {
  event.preventDefault();
  const draft = composer();
  if (state.submittingTask || !draft.target || !draft.method || byId("compose-submit").disabled) return;
  let params;
  if (draft.format === "json") {
    try {
      params = JSON.parse(draft.json);
      if (!params || Array.isArray(params) || typeof params !== "object") throw new TypeError();
    } catch {
      setMessage("compose-message", t("requireObject"));
      byId("json-input").focus();
      return;
    }
  } else {
    if (!draft.prompt?.trim()) {
      setMessage("compose-message", t("requirePrompt"));
      byId("prompt-input").focus();
      return;
    }
    params = { prompt: draft.prompt };
  }
  setMessage("compose-message", "");
  const composeKey = state.composeKey;
  const sentInput = [draft.prompt, draft.json];
  state.submittingTask = true;
  renderComposer();
  try {
    const args = { target: draft.target, method: draft.method, params };
    if (draft.session) args.session_id = draft.session;
    const job = await api("invoke", args);
    switchView("tasks", { load: false });
    selectJob(job.job_id, { query: false, detail: job });
    if (job.invoke_error && job.invoke_error.code !== "OUTCOME_UNKNOWN") {
      if (state.composeKey === composeKey) setMessage("compose-message", problem(job.invoke_error));
    } else {
      if (draft.prompt === sentInput[0] && draft.json === sentInput[1]) {
        draft.prompt = "";
        draft.json = "";
      }
      persist();
      const dialog = byId("compose");
      if (state.composeKey === composeKey && dialog.open) {
        composerFocus = byId("detail-panel");
        dialog.close();
      } else if (!dialog.open) {
        byId("detail-panel").focus();
      }
      announce(t("taskSent"));
    }
    await refreshLists();
  } catch (error) {
    if (state.composeKey === composeKey) setMessage("compose-message", errorMessage(error));
  } finally {
    state.submittingTask = false;
    if (byId("compose").open) renderComposer();
  }
}

async function submitReply(event) {
  event.preventDefault();
  const id = state.currentReply;
  if (!id || state.submittingReply || byId("reply-submit").disabled) return;
  const text = byId("reply-input").value;
  if (!text.trim()) {
    setMessage("reply-message", t("requireReply"));
    byId("reply-input").focus();
    return;
  }
  state.submittingReply = id;
  state.replyErrors.delete(id);
  renderDetails();
  try {
    const job = await api("answer", { job_id: id, text });
    if (job.invoke_error && job.invoke_error.code !== "OUTCOME_UNKNOWN") {
      state.replyErrors.set(id, job.invoke_error);
    } else {
      state.sentAnswers.set(id, job.job_id);
      announce(t("replySent"));
    }
    await refreshLists();
  } catch (error) {
    state.replyErrors.set(id, error);
  } finally {
    state.submittingReply = null;
    render();
  }
}

function chooseFilter(filter) {
  state.filter = filter;
  if (filter === "waiting" && questionState(state.jobs.get(state.selected)) !== "waiting") {
    const first = [...state.jobs.values()].find((job) => questionState(job) === "waiting");
    if (first) {
      selectJob(first.job_id);
      return;
    }
  }
  persist();
  renderQueue();
}

byId("language").addEventListener("click", () => {
  state.locale = !state.locale;
  persist();
  applyLanguage();
  render();
});
byId("refresh").addEventListener("click", () => {
  void refreshLists();
  void refreshDetail();
});
byId("nodes-nav").addEventListener("click", () => switchView("nodes"));
byId("tasks-nav").addEventListener("click", () => switchView("tasks"));
byId("recent-filter").addEventListener("click", () => chooseFilter("recent"));
byId("waiting-filter").addEventListener("click", () => chooseFilter("waiting"));
byId("new-task").addEventListener("click", (event) => openComposer({ opener: event.currentTarget }));
byId("close-compose").addEventListener("click", () => byId("compose").close());
byId("cancel-compose").addEventListener("click", () => byId("compose").close());
byId("compose").addEventListener("close", () => {
  if (byId("compose").open) return;
  state.composeKey = null;
  persist();
  restoreComposerFocus();
});
byId("target").addEventListener("change", (event) => {
  composer().target = event.target.value;
  composer().method = "";
  persist();
  setMessage("compose-message", "");
  renderComposer();
});
byId("method").addEventListener("change", (event) => {
  composer().method = event.target.value;
  persist();
  setMessage("compose-message", "");
  renderComposer();
});
byId("format").addEventListener("change", (event) => {
  composer().format = event.target.value;
  persist();
  setMessage("compose-message", "");
  renderComposer();
});
for (const [id, field] of [["prompt-input", "prompt"], ["json-input", "json"]]) {
  byId(id).addEventListener("input", (event) => {
    composer()[field] = event.target.value;
    persist();
  });
}
byId("reply-input").addEventListener("input", (event) => {
  if (!state.currentReply) return;
  state.drafts[state.currentReply] = event.target.value;
  state.replyErrors.delete(state.currentReply);
  persist();
  renderDetails();
});
byId("compose-form").addEventListener("submit", submitTask);
byId("reply-form").addEventListener("submit", submitReply);

applyLanguage();
if (access.fresh) persist();
render();
if (state.token) {
  void refreshLists().then(() => refreshDetail());
  if (state.composeKey) showComposer();
}
setInterval(() => {
  if (document.visibilityState === "visible") void refreshLists();
}, pollInterval);
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") void refreshLists();
});
