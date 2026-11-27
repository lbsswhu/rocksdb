#!/usr/bin/env python3
"""Render the design figures and an offline HTML reading copy.

Documentation-only dependencies: Markdown, matplotlib. Example:
  python3 -m pip install --target /tmp/async-memtable-doc-tools Markdown matplotlib
  PYTHONPATH=/tmp/async-memtable-doc-tools python3 docs/design/async_memtable_write/render.py
SVG diagrams use native text. The benchmark figure reads the archived CSVs.
"""

from pathlib import Path
import csv
import html
import re

import markdown
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager


HERE = Path(__file__).resolve().parent
DESIGN = HERE.parent
ROOT = DESIGN.parent.parent
BLUE = "#2563eb"
TEAL = "#0f766e"
AMBER = "#b45309"
RED = "#b91c1c"
INK = "#172b4d"
MUTED = "#52647b"


class Figure:
    def __init__(self, name, title, height=620):
        self.name = name
        self.height = height
        self.items = [
            '<svg xmlns="http://www.w3.org/2000/svg" width="1200" '
            f'height="{height}" viewBox="0 0 1200 {height}" role="img">',
            f"<title>{html.escape(title)}</title>",
            '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" '
            'markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
            '<path d="M 0 0 L 10 5 L 0 10 z" fill="context-stroke"/></marker></defs>',
            '<style>text{font-family:"WenQuanYi Zen Hei","Noto Sans CJK SC",'
            '"Microsoft YaHei",sans-serif} .small{font-size:15px}</style>',
            f'<rect width="1200" height="{height}" fill="#f8fafc"/>',
        ]
        self.text(35, 43, title, size=25, color=INK, anchor="start", bold=True)

    def text(self, x, y, content, size=17, color=INK, anchor="middle", bold=False):
        weight = "600" if bold else "400"
        for i, line in enumerate(content.split("\n")):
            self.items.append(
                f'<text x="{x}" y="{y + i * (size + 8)}" text-anchor="{anchor}" '
                f'font-size="{size}" font-weight="{weight}" fill="{color}">'
                f"{html.escape(line)}</text>"
            )

    def box(self, x, y, w, h, content, color=BLUE, fill="#eff6ff", size=17):
        self.items.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" '
            f'fill="{fill}" stroke="{color}" stroke-width="1.5"/>'
        )
        lines = content.count("\n") + 1
        self.text(x + w / 2, y + h / 2 - (lines - 1) * (size + 8) / 2 + size * .35,
                  content, size=size)

    def line(self, x1, y1, x2, y2, color=MUTED, dash=False, arrow=True):
        self.items.append(
            f'<path d="M{x1},{y1} L{x2},{y2}" fill="none" stroke="{color}" '
            f'stroke-width="1.8"' + (' stroke-dasharray="7 5"' if dash else '') +
            (' marker-end="url(#arrow)"' if arrow else '') + '/>'
        )

    def path(self, points, color=MUTED, dash=False):
        d = "M" + " L".join(f"{x},{y}" for x, y in points)
        self.items.append(
            f'<path d="{d}" fill="none" stroke="{color}" stroke-width="1.8" '
            'marker-end="url(#arrow)"' + (' stroke-dasharray="7 5"' if dash else '') + '/>'
        )

    def save(self):
        (HERE / (self.name + ".svg")).write_text("\n".join(self.items + ["</svg>"]), encoding="utf-8")


def architecture():
    f = Figure("architecture", "01  整体架构：生产、应用、发布与通知", 700)
    f.box(40, 95, 240, 86, "前台 Writer / WriteGroup\nWAL 队列形成逻辑单生产者")
    f.box(360, 95, 320, 86, "WAL 前准备 owned group\n校验 / 合并 / 引用 / 预算 / 节点")
    f.box(790, 95, 340, 86, "写 WAL / 按需 Sync\n成功后提交任务，交接 WAL gate")
    f.line(280, 138, 360, 138)
    f.line(680, 138, 790, 138)
    f.box(790, 245, 340, 75, "AsyncCommitQueue\n整组任务；只传 shared_ptr")
    f.line(960, 181, 960, 245)
    f.box(610, 390, 235, 88, "worker 0\n逐 range 应用一个完整组", TEAL, "#f0fdfa")
    f.box(900, 390, 235, 88, "worker 1…N−1\n与其他 group 并发", TEAL, "#f0fdfa")
    f.path([(900, 320), (900, 350), (728, 350), (728, 390)])
    f.path([(1020, 320), (1020, 390)])
    f.box(355, 565, 490, 84, "多 worker：SequenceNotifier\n清理完成报告 → 按 group id 排序 → 连续发布 P/D", AMBER, "#fffbeb")
    f.path([(728, 478), (728, 565)], TEAL)
    f.path([(1020, 478), (1020, 607), (845, 607)], TEAL)
    f.box(40, 390, 290, 88, "读 / 强可见性写等待者\n固定目标 T，等待 P ≥ T")
    f.path([(355, 607), (185, 607), (185, 478)], AMBER)
    f.text(185, 560, "通知覆盖区间的等待槽", size=16, color=AMBER)
    f.box(40, 245, 460, 75, "单 worker 特例：worker 自己逐组发布 P\nnotifier 仅负责等待槽通知", TEAL, "#f0fdfa")
    f.text(43, 681, "三个边界：WAL 确认 ≠ sequence 发布 ≠ 资源 drain；均不等于 SST flush。", size=17, anchor="start")
    f.save()


def write_sequence():
    f = Figure("write_sequence", "02  多 worker 写入时序与两种返回边界", 795)
    xs = [115, 395, 715, 1050]
    for x, label in zip(xs, ["调用者 / leader", "WAL / executor queue", "MemTable worker", "notifier"]):
        f.box(x - 110, 80, 220, 48, label)
        f.line(x, 130, x, 735, dash=True, arrow=False)
    messages = [
        (180, 0, 1, "WAL 前准备 owned group / 任务节点", BLUE),
        (245, 1, 1, "分配 sequence，更新 A；写 WAL / Sync", BLUE),
        (310, 1, 2, "WAL 成功，整组入队；WakeOne", BLUE),
        (375, 1, 0, "释放 WAL gate / 通知调用者", BLUE),
        (455, 2, 2, "应用整组全部 range，清理 payload / 引用", TEAL),
        (520, 2, 3, "CAS 提交完成报告；所有权转移", TEAL),
        (585, 3, 3, "补齐连续前缀，发布 P，推进 D", AMBER),
        (675, 3, 0, "通知等待槽，P ≥ 目标则结束等待", AMBER),
    ]
    for y, src, dst, label, color in messages:
        if src == dst:
            x = xs[src]
            f.path([(x, y), (x + 75, y), (x + 75, y + 30), (x, y + 30)], color)
            # Self-message text placed on the free side of its lifeline.
            if src == 1:
                f.text(x + 100, y - 12, label, size=15, color=color, anchor="start")
            else:
                f.text(x - 18, y - 12, label, size=15, color=color, anchor="end")
        else:
            f.line(xs[src], y, xs[dst], y, color)
            f.text((xs[src] + xs[dst]) / 2, y - 12, label, size=15, color=color)
    f.box(15, 396, 235, 65, "lazy=true 可返回\n无需等待 worker 完成", TEAL, "#f0fdfa", 16)
    f.box(15, 700, 235, 65, "lazy=false 发布后返回\n等待发生在 WAL gate 外", AMBER, "#fffbeb", 16)
    f.text(450, 768, "单 worker：worker 逐组发布 P，批量清理后再推进 D。", size=16, anchor="start")
    f.save()


def publication():
    f = Figure("publication", "03  乱序应用、零记录组与连续前缀", 570)
    f.text(45, 101, "WAL 顺序", anchor="start", bold=True)
    for x, label in [(240, "G1：sequence 101–110"), (555, "G2：仅 LogData，零记录"), (870, "G3：sequence 111–120")]:
        f.box(x, 75, 280, 74, label, size=16)
    f.text(45, 203, "初始状态", anchor="start", bold=True)
    f.text(240, 203, "P=100，Q=D=0；已经分配 A=120。", anchor="start")
    f.box(240, 240, 380, 80, "G2、G3 先完成并清理\nnotifier 保留排序元数据", TEAL, "#f0fdfa")
    f.box(760, 240, 390, 80, "G1 仍未完成：存在前序空洞\nP=100，Q=D=0", RED, "#fef2f2")
    f.line(620, 280, 760, 280)
    f.box(240, 380, 380, 80, "G1 后完成并报告\n完成链表现在为 G1 → G2 → G3", TEAL, "#f0fdfa")
    f.line(430, 320, 430, 380)
    f.box(760, 380, 390, 80, "连续退休 G1 / G2 / G3\nP=120，Q=D=3", AMBER, "#fffbeb")
    f.line(620, 420, 760, 420)
    f.text(45, 515, "G2 不消耗 sequence，却必须消耗 group ticket 并参加排序、背压和 drain。", size=18, anchor="start")
    f.save()


def pipeline():
    f = Figure("pipeline", "04  pipelined write 与三种执行形态", 775)
    lanes = [
        (100, "原生 pipeline=true", "WAL group G1", "WAL group G2", "调用者应用 G1", "调用者应用 G2"),
        (290, "async，threads=0，pipeline=true", "WAL group G1", "WAL group G2", "代表节点 G1 → 原 leader 应用", "代表节点 G2 → 原 leader 应用"),
        (480, "async，threads>1，pipeline 任意（threads=1 时组间顺序应用）", "WAL G1 → executor", "WAL G2 → executor", "worker 0 应用 G1", "worker 1 应用 G2"),
    ]
    for y, label, wal1, wal2, mem1, mem2 in lanes:
        f.text(35, y, label, anchor="start", bold=True)
        f.text(40, y + 55, "WAL", anchor="start", size=16)
        f.text(40, y + 112, "MemTable", anchor="start", size=16)
        f.box(200, y + 23, 250, 44, wal1, size=15)
        f.box(470, y + 23, 250, 44, wal2, size=15)
        f.path([(450, y + 46), (450, y + 80)], TEAL)
        if y == 480:
            f.box(450, y + 80, 420, 44, mem1, TEAL, "#f0fdfa", 15)
            f.box(720, y + 140, 390, 44, mem2, TEAL, "#f0fdfa", 15)
            f.path([(720, y + 67), (720, y + 73), (1125, y + 73), (1125, y + 135), (920, y + 135), (920, y + 140)], TEAL)
        else:
            f.box(450, y + 80, 300, 44, mem1, TEAL, "#f0fdfa", 15)
            f.box(750, y + 80, 350, 44, mem2, TEAL, "#f0fdfa", 15)
    f.text(35, 728, "时间向右 →    每组先完成 WAL 再应用；下一组 WAL 与前组应用重叠。", size=17, anchor="start")
    f.text(35, 757, "有池时不再进入原生 MemTable writer 队列；多 worker 的应用区间可以重叠。", size=17, anchor="start")
    f.save()


def ownership():
    f = Figure("ownership", "05  一次合并、零 payload 交接与生命周期", 640)
    for x, label in [(40, "调用者 batch A"), (310, "调用者 batch B"), (580, "调用者 batch C")]:
        f.box(x, 100, 220, 66, label)
    f.text(900, 127, "调用者拥有的内存\n成功返回后允许销毁/复用", size=16, color=MUTED, anchor="start")
    for x in [150, 420, 690]:
        f.path([(x, 166), (x, 205), (430, 205), (430, 240)])
    f.text(775, 209, "此处发生一次 owned 合并复制", size=17, color=AMBER, anchor="start")
    f.box(170, 240, 690, 106, "AsyncCommitGroup 独立持有的 WriteBatch\nHeader | range A | range B | range C\nranges 只保存字节 offset、record index、sequence 和选项", TEAL, "#f0fdfa")
    f.box(35, 415, 300, 75, "WAL writer\n直接读取 apply_batch")
    f.box(450, 415, 310, 75, "executor queue node\nshared_ptr 指向 group")
    f.box(875, 415, 290, 75, "MemTable worker\n按原 buffer 的 offset 解析")
    f.path([(300, 346), (185, 415)])
    f.path([(605, 346), (605, 415)])
    f.line(760, 452, 875, 452, TEAL)
    f.text(817, 405, "无 payload 复制", size=15, color=TEAL)
    f.box(170, 540, 900, 62, "CF / MemTable 另行取引用；后台不再访问原 Writer、WriteGroup 或用户 CF handle", AMBER, "#fffbeb", 17)
    f.save()


def wait_protocol():
    f = Figure("wait_protocol", "06  sequence 等待与按需通知协议", 690)
    f.box(40, 95, 480, 65, "入口固定目标 T；先检查 P ≥ T")
    f.box(40, 210, 480, 80, "最多 rounds 次轮询\n每轮 Uniform(delay) × multiplier 次 pause")
    f.line(280, 160, 280, 210)
    f.box(40, 345, 480, 80, "锁 slot[T % slots]，锁内重查 P\n注册 waiter，再进入 CV wait")
    f.line(280, 290, 280, 345)
    f.box(40, 495, 480, 80, "被唤醒后重查 P / error / deadline\nP ≥ T 才成功结束等待")
    f.line(280, 425, 280, 495)
    f.box(700, 95, 450, 80, "worker 或 notifier 推进 P\n发送 progress epoch 信号", TEAL, "#f0fdfa")
    f.box(700, 240, 450, 95, "notifier：捕获本轮 published\n扫描 (N, published] 覆盖的 slots\n逐槽加锁、通知已有 waiter", AMBER, "#fffbeb")
    f.line(925, 175, 925, 240)
    f.path([(700, 290), (620, 290), (620, 530), (520, 530)], AMBER)
    f.box(700, 410, 450, 80, "完成通知后才令 N = 本轮 published\n通知期间的新发布留到下一轮", AMBER, "#fffbeb")
    f.line(925, 335, 925, 410)
    f.text(700, 553, "无 waiter：单 worker notifier 休眠\n多 worker：仍须处理完成报告和退休", size=16, anchor="start")
    f.text(40, 635, "slot 锁防止 CV 丢唤醒；event 的 register → epoch → recheck → wait 防止事件丢唤醒。", size=18, anchor="start")
    f.save()


def lifecycle():
    f = Figure("lifecycle", "07  两条完成路径与错误退出", 660)
    f.box(40, 105, 220, 75, "准备 owned group\n引用 / 预算 / 任务节点")
    f.box(355, 105, 220, 75, "WAL 成功\n提交整组任务")
    f.box(680, 105, 220, 75, "worker 应用完成\n全部 range 已处理", TEAL, "#f0fdfa")
    f.line(260, 142, 355, 142)
    f.line(575, 142, 680, 142)
    f.box(350, 275, 290, 78, "单 worker：逐组发布 P/Q\n等待者可先于清理结束", TEAL, "#f0fdfa")
    f.box(810, 275, 325, 78, "多 worker：先清理 payload / 引用\n再向 notifier 提交完成报告", AMBER, "#fffbeb")
    f.path([(740, 180), (740, 220), (495, 220), (495, 275)], TEAL)
    f.path([(825, 180), (970, 275)], AMBER)
    f.box(350, 420, 290, 78, "lease 末批量清理、归还预算\n连续推进 drain D", TEAL, "#f0fdfa")
    f.line(495, 353, 495, 420)
    f.box(810, 420, 325, 78, "notifier 排序、补齐前缀\n发布 P/Q、退休 D、归还预算", AMBER, "#fffbeb")
    f.line(973, 353, 973, 420)
    f.box(35, 305, 230, 160, "WAL / apply 终止错误\n\n冻结新的 P/Q 发布\n继续清理与退休\n唤醒读写和容量等待者", RED, "#fef2f2", 16)
    f.path([(150, 180), (150, 305)], RED, dash=True)
    f.text(350, 579, "管理操作与 Close 等待 drain，不能用 P 已推进替代资源清理。", anchor="start", size=18)
    f.text(350, 614, "多 worker 的 drain 完成前，notifier 必须保持运行。", anchor="start", size=18)
    f.save()


def benchmark():
    font_path = Path("/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc")
    if font_path.exists():
        font_manager.fontManager.addfont(str(font_path))
        plt.rcParams["font.family"] = font_manager.FontProperties(fname=str(font_path)).get_name()
    plt.rcParams.update({"axes.unicode_minus": False, "svg.hashsalt": "async-memtable-design", "font.size": 11})
    with (ROOT / "benchmark_results/async_memtable_write/summary.csv").open() as src:
        rows = list(csv.DictReader(src))
    conditions = [("overwrite", "False"), ("overwrite", "True"), ("readrandomwriterandom", "False"), ("readrandomwriterandom", "True")]
    ratios = {"True": [], "False": []}
    for workload, sync in conditions:
        group = [r for r in rows if r["workload"] == workload and r["sync"] == sync]
        native = max(float(r["median_ops"]) for r in group if r["async_"] == "False")
        for lazy in ratios:
            best = max(float(r["median_ops"]) for r in group if r["async_"] == "True" and r["lazy"] == lazy)
            ratios[lazy].append(best / native)
    with (ROOT / "benchmark_results/ycsb_async_full/summary.csv").open() as src:
        yrows = list(csv.DictReader(src))
    yratios = []
    for workload in "abcdef":
        group = [r for r in yrows if r["workload"] == workload]
        native = max(float(r["median_ops"]) for r in group if r["name"].startswith("native"))
        best = max(float(r["median_ops"]) for r in group if r["name"].startswith("async"))
        yratios.append(best / native)
    fig, axes = plt.subplots(1, 2, figsize=(13, 5.2), gridspec_kw={"width_ratios": [1.2, 1]})
    fig.patch.set_facecolor("#f8fafc")
    for ax in axes:
        ax.set_facecolor("#f8fafc")
        ax.axhline(1, color=MUTED, linestyle="--", linewidth=1)
        ax.spines[["top", "right"]].set_visible(False)
        ax.set_ylabel("相对本实验最佳原生吞吐")
        ax.set_ylim(0, 2.02)
        ax.grid(axis="y", alpha=.15)
        ax.set_axisbelow(True)
    for shift, (lazy, values) in zip([-.18, .18], ratios.items()):
        bars = axes[0].bar([i + shift for i in range(4)], values, width=.34,
                           label="lazy=" + lazy.lower(), color=TEAL if lazy == "True" else BLUE)
        axes[0].bar_label(bars, labels=[f"{(v - 1) * 100:+.1f}%" for v in values], padding=4, fontsize=9)
    axes[0].set_xticks(range(4), ["纯写\nsync=false", "纯写\nsync=true", "读写混合\nsync=false", "读写混合\nsync=true"])
    axes[0].set_title("db_bench：三轮中位数，最佳配置分别取值")
    axes[0].legend(loc="upper right", frameon=False)
    bars = axes[1].bar(list("ABCDEF"), yratios, width=.6, color=AMBER)
    axes[1].bar_label(bars, labels=[f"{(v - 1) * 100:+.1f}%" for v in yratios], padding=4, fontsize=10)
    axes[1].set_title("完整 YCSB：每组单轮，最佳 async 候选")
    fig.suptitle("08  性能收益具有负载与确认模式边界", fontsize=17, x=.04, ha="left")
    fig.text(.05, .015, "数据来自仓库归档 CSV；图中最优配置可能不同。db_bench 吞吐包含 drain；YCSB 单轮不提供统计显著性结论。", fontsize=10, color=MUTED)
    fig.tight_layout(rect=(0, .05, 1, .94))
    fig.savefig(HERE / "benchmark.svg", metadata={"Date": None})
    plt.close(fig)
    output = HERE / "benchmark.svg"
    output.write_text("\n".join(line.rstrip() for line in output.read_text(encoding="utf-8").splitlines()) + "\n", encoding="utf-8")


def reading_copy():
    source = (DESIGN / "async_memtable_write_design.md").read_text(encoding="utf-8")
    converter = markdown.Markdown(extensions=["tables", "fenced_code", "toc"], extension_configs={"toc": {"toc_depth": "2-3"}})
    body = converter.convert(source)
    # Inline every figure so the HTML remains readable when copied by itself.
    def embed(match):
        figure = (DESIGN / html.unescape(match.group(1))).resolve()
        assert figure.parent == HERE and figure.suffix == ".svg"
        svg = figure.read_text(encoding="utf-8")
        svg = re.sub(r"<\?xml.*?\?>|<!DOCTYPE.*?\]?>", "", svg, flags=re.S)
        return '<figure>' + svg + '</figure>'
    body = re.sub(r'<img[^>]*src="(async_memtable_write/[^"<>]+\.svg)"[^>]*\s*/?>', embed, body)
    css = """
      :root{color-scheme:light;--ink:#172b4d;--muted:#52647b;--line:#dce4ef}
      *{box-sizing:border-box}body{margin:0;background:#f3f6fb;color:var(--ink);font:16px/1.85 system-ui,'Microsoft YaHei',sans-serif}
      aside{position:fixed;inset:0 auto 0 0;width:265px;padding:24px 18px;background:#fff;border-right:1px solid var(--line);overflow:auto;font-size:13px}
      aside h2{font-size:16px;margin:0 0 16px}aside ul{list-style:none;padding-left:12px}aside>div>ul{padding-left:0}aside li{margin:5px 0}
      a{color:#1d4ed8;text-decoration:none}a:hover{text-decoration:underline}main{margin:0 30px 0 295px;max-width:1250px;background:#fff;padding:45px 48px 70px}
      h1{font-size:32px;line-height:1.4;margin:0 0 32px}h2{font-size:27px;margin-top:56px;padding-bottom:9px;border-bottom:2px solid #e2e8f0}
      h3{font-size:22px;margin-top:40px}h4{font-size:18px;margin-top:30px}p{margin:16px 0}table{border-collapse:collapse;width:100%;font-size:14px;line-height:1.65;margin:22px 0;overflow-wrap:anywhere}
      th,td{border:1px solid var(--line);padding:10px 12px;vertical-align:top}th{background:#edf3fc;text-align:left}tr:nth-child(even){background:#fafcff}
      code{background:#eef2f8;border-radius:3px;padding:2px 5px;font-size:.88em;overflow-wrap:anywhere}pre{overflow-x:auto;background:#14233d;color:#e2e8f0;border-radius:8px;padding:18px;font-size:14px;line-height:1.65}pre code{background:transparent;padding:0;overflow-wrap:normal}
      figure{margin:24px 0;padding:4px;background:#f8fafc;border:1px solid var(--line);border-radius:10px;overflow:auto}figure svg{width:100%;height:auto;min-width:650px;display:block}em{color:var(--muted);font-size:14px}
      .banner{color:var(--muted);font-size:13px;border-bottom:1px solid var(--line);padding-bottom:15px;margin-bottom:26px}
      @media(max-width:1000px){aside{position:static;width:auto;max-height:300px}main{margin:0;padding:24px 20px}figure svg{min-width:600px}}
      @media print{aside,.banner{display:none}body{background:#fff;font-size:11px}main{margin:0;padding:0;max-width:none}h2{break-before:page}h3,h4{break-after:avoid}figure{break-inside:avoid}figure svg{min-width:0}pre{white-space:pre-wrap}a{color:inherit}table{font-size:10px}}
    """
    result = ('<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">'
              '<meta name="viewport" content="width=device-width,initial-scale=1">'
              '<title>RocksDB Async Memtable Write 特性设计说明书</title><style>' + css +
              '</style></head><body><aside><h2>设计说明书 · 目录</h2>' + converter.toc +
              '</aside><main><div class="banner">分析基线 ff629fb02 · 产品基线 97fc7d395 · '
              '2026-11-27 · 离线阅读版，插图已内嵌 · 可用浏览器打印</div>' + body + '</main></body></html>')
    result = "\n".join(line.rstrip() for line in result.splitlines()) + "\n"
    (DESIGN / "async_memtable_write_design.html").write_text(result, encoding="utf-8")


if __name__ == "__main__":
    for render in (architecture, write_sequence, publication, pipeline, ownership, wait_protocol, lifecycle, benchmark):
        render()
    reading_copy()
    print("Rendered 8 SVG figures and the offline HTML reading copy.")
