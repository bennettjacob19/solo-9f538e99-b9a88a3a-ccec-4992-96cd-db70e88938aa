(() => {
  const $ = (id) => document.getElementById(id);
  const form = $("check-form");
  const notice = $("notice");
  let pollTimer = null;

  const fmt = (n) => {
    if (n === null || n === undefined) return "-";
    if (typeof n !== "number") return String(n);
    return n.toLocaleString("en-US");
  };

  const bytes = (n) => {
    if (typeof n !== "number") return "-";
    if (n >= 1024 * 1024 && n % (1024 * 1024) === 0) return `${fmt(n)} (${n / (1024 * 1024)} MiB)`;
    if (n >= 1024 && n % 1024 === 0) return `${fmt(n)} (${n / 1024} KiB)`;
    return fmt(n);
  };

  function showNotice(text, kind = "error") {
    notice.hidden = false;
    notice.className = `notice ${kind}`;
    notice.textContent = text;
  }
  function clearNotice() {
    notice.hidden = true;
    notice.textContent = "";
  }

  function formPayload() {
    const data = new FormData(form);
    const payload = {
      size_bytes: Number(data.get("size_bytes")),
      scenario: data.get("scenario"),
      max_requests_per_side: 16,
      max_combined_body_bytes: 16384,
    };
    const raw = String(data.get("diff_block") || "").trim();
    if (raw !== "") payload.diff_block = Number(raw);
    return payload;
  }

  function setBusy(running) {
    $("start-btn").disabled = running;
    $("cancel-btn").disabled = !running;
  }

  function renderEvents(events) {
    const list = $("events");
    list.innerHTML = "";
    (events || []).slice(-100).forEach((e) => {
      const li = document.createElement("li");
      const t = document.createElement("span");
      t.className = "t";
      t.textContent = (e.at || "").slice(11, 23);
      const k = document.createElement("span");
      k.className = `kind ${e.kind === "error" ? "error" : ""}`;
      k.textContent = e.kind;
      const m = document.createElement("span");
      m.textContent = e.message;
      li.append(t, k, m);
      list.append(li);
    });
    list.scrollTop = list.scrollHeight;
  }

  function renderSide(prefix, side) {
    const s = side || {};
    $(`${prefix}-etag`).textContent = s.locked_etag || "-";
    $(`${prefix}-version`).textContent = s.locked_version || (s.lock_status === "failed" ? "锁定失败" : "-");
    $(`${prefix}-current`).textContent = s.current_version || "-";
    $(`${prefix}-size`).textContent = s.size
      ? `${fmt(s.size)} / ${fmt(s.blocks)} 块`
      : "-";
    const served = s.locked_served;
    $(`${prefix}-served`).textContent =
      served === true ? "是（已换版，按锁定版应答）" :
      served === false ? "是（当前版即锁定版）" :
      "-";
    $(`${prefix}-served`).className = "v " + (served === true ? "incomplete" : "match");
    $(`${prefix}-req`).textContent = fmt(s.requests || 0);
    $(`${prefix}-body`).textContent = fmt(s.body_bytes || 0);
  }

  function renderDifferences(diffs, outcome) {
    const body = $("diff-body");
    body.innerHTML = "";
    const list = diffs || [];
    $("no-diff").hidden = list.length > 0;
    $("no-diff").textContent =
      outcome === "incomplete" ? "未核完，已确认的差异为零。"
      : outcome === "pending" ? "核对进行中…"
      : "未发现差异。";
    $("diff-table").hidden = list.length === 0;
    list.forEach((d, i) => {
      const tr = document.createElement("tr");
      const cells = [
        String(i + 1),
        `${fmt(d.start)}`,
        `${fmt(d.end)}`,
        `${fmt(d.length)}`,
        d.blocks ? `${d.blocks} 块` : "-",
      ];
      cells.forEach((text, idx) => {
        const td = document.createElement("td");
        td.textContent = text;
        if (idx === 1 || idx === 2 || idx === 3) td.className = "mono";
        tr.append(td);
      });
      body.append(tr);
    });
  }

  function render(state) {
    const status = state.status || "idle";
    $("status").textContent =
      status === "idle" ? "待运行" :
      status === "running" ? "核对中…" :
      status === "done" ? "已完成" : status;
    const outcome = state.outcome || "pending";
    $("outcome").textContent = state.outcome_label || outcome;
    $("outcome").className =
      "v " + (outcome === "identical" ? "match" :
              outcome === "different" || outcome === "incomplete" ? "incomplete"
              : "pending");
    const params = state.params || {};
    $("scenario").textContent = params.scenario_label || params.scenario || "-";
    $("size-info").textContent = params.size_bytes
      ? `${bytes(params.size_bytes)} / ${fmt(state.block_size)} B`
      : "-";
    $("diff-summary").textContent = state.params
      ? `${fmt(state.difference_count || 0)} 段 / ${bytes(state.difference_bytes || 0)}`
        + (state.outcome === "incomplete" ? "（未核完，仅为已确认部分）" : "")
      : "-";
    $("rounds").textContent = state.probe_rounds !== undefined ? fmt(state.probe_rounds) : "-";
    $("elapsed").textContent = state.elapsed_ms !== undefined ? `${fmt(state.elapsed_ms)} ms` : "-";
    $("error").textContent = state.error || "-";
    $("error").className = "v " + (state.error ? "incomplete" : "");

    const sides = state.sides || {};
    renderSide("a", sides.a);
    renderSide("b", sides.b);

    const budget = state.budget || {};
    $("body-total").textContent = fmt(budget.combined_body_bytes || 0);
    $("req-cap").textContent = fmt(budget.max_requests_per_side || 16);
    $("body-cap").textContent = fmt(budget.max_combined_body_bytes || 16384);
    $("req-headroom").textContent = fmt(budget.requests_headroom || 0);
    $("body-headroom").textContent = fmt(budget.body_headroom || 0);

    renderDifferences(state.differences, outcome);

    const swapped = state.swapped || [];
    const swapEl = $("swap-notice");
    if (swapped.length) {
      swapEl.hidden = false;
      swapEl.textContent = swapped
        .map((s) => `${(s.side || "").toUpperCase()} 端 ${s.from} → ${s.to}：${s.note || ""}`)
        .join("；");
    } else {
      swapEl.hidden = true;
    }

    renderEvents(state.events);

    if (state.error && status === "done") showNotice(`核对未完成：${state.error}`);
    else clearNotice();

    setBusy(status === "running");
  }

  async function poll() {
    try {
      const resp = await fetch("/api/check");
      const state = await resp.json();
      render(state);
      if (state.status === "running") {
        pollTimer = setTimeout(poll, 250);
      } else {
        pollTimer = null;
      }
    } catch (err) {
      showNotice(`无法读取状态：${err}`);
      pollTimer = setTimeout(poll, 1000);
    }
  }

  function startPolling() {
    if (pollTimer) clearTimeout(pollTimer);
    pollTimer = null;
    poll();
  }

  form.addEventListener("submit", async (ev) => {
    ev.preventDefault();
    clearNotice();
    setBusy(true);
    try {
      const resp = await fetch("/api/check", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(formPayload()),
      });
      if (!resp.ok) {
        const body = await resp.json().catch(() => ({}));
        throw new Error(body.detail ? JSON.stringify(body.detail) : `HTTP ${resp.status}`);
      }
      startPolling();
    } catch (err) {
      setBusy(false);
      showNotice(`启动失败：${err.message || err}`);
    }
  });

  $("preset-128").addEventListener("click", async () => {
    const fields = form.elements;
    fields.namedItem("scenario").value = "single_block_diff";
    fields.namedItem("size_bytes").value = "134217728";
    fields.namedItem("diff_block").value = "";
    clearNotice();
    setBusy(true);
    try {
      const resp = await fetch("/api/check", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ size_bytes: 134217728, scenario: "single_block_diff" }),
      });
      if (!resp.ok) {
        const body = await resp.json().catch(() => ({}));
        throw new Error(body.detail ? JSON.stringify(body.detail) : `HTTP ${resp.status}`);
      }
      startPolling();
    } catch (err) {
      setBusy(false);
      showNotice(`启动失败：${err.message || err}`);
    }
  });

  $("cancel-btn").addEventListener("click", async () => {
    try {
      const resp = await fetch("/api/check/cancel", { method: "POST" });
      const body = await resp.json();
      render(body.state || {});
      if (pollTimer) { clearTimeout(pollTimer); pollTimer = null; }
    } catch (err) {
      showNotice(`取消失败：${err.message || err}`);
    }
  });

  poll();
})();
