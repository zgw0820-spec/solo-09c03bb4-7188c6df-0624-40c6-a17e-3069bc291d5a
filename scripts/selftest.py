#!/usr/bin/env python3
"""租约服务端到端自测（仅依赖标准库）。

覆盖：申请 / 同号幂等重试（跨实例）/ 一机一址 / 池满“地址不足” / 续租（含错误令牌）/
释放（幂等）/ 过期后旧号重试与旧令牌迟到操作 / 地址回收再分配 /
双实例争抢最后一个地址 / 双实例满池争抢 / 申请记录审计。

前提：两个实例已启动且共享同一数据库，地址池为 10.99.0.0/29（6 个地址），ALLOW_RESET=1。
用法：python3 scripts/selftest.py   （可用 BASE1/BASE2 环境变量覆盖地址）
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE1 = os.environ.get("BASE1", "http://127.0.0.1:8001")
BASE2 = os.environ.get("BASE2", "http://127.0.0.1:8002")

passed = 0
failed = 0


def check(name, cond, extra=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"    PASS  {name}")
    else:
        failed += 1
        print(f"    FAIL  {name}  {extra}")


def req(method, base, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(base + path, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=15) as resp:
            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw else {}), resp.headers.get("X-Served-By")
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"raw": raw}
        return e.code, payload, (e.headers.get("X-Served-By") if e.headers else None)


def apply(base, vm, rq, ttl):
    return req("POST", base, "/api/leases",
               {"vm_id": vm, "request_id": rq, "ttl_seconds": ttl})


def pool(base):
    return req("GET", base, "/api/pool")[1]


def main():
    print(f"== 目标实例: {BASE1} / {BASE2}\n")

    print("-- P0 健康检查 / 参数校验 / 重置")
    s, b, _ = req("GET", BASE1, "/api/health")
    check("node1 健康", s == 200 and b.get("node") == "node1", f"{s} {b}")
    s, b, _ = req("GET", BASE2, "/api/health")
    check("node2 健康", s == 200 and b.get("node") == "node2", f"{s} {b}")
    s, b, _ = apply(BASE1, "vm-bad", "req-bad-0", 0)
    check("租期 0 秒被拒绝(422)", s == 422, f"{s} {b}")
    s, b, _ = apply(BASE1, "vm-bad", "req-bad-61", 61)
    check("租期 61 秒被拒绝(422)", s == 422, f"{s} {b}")
    s, b, _ = req("POST", BASE2, "/api/admin/reset")
    check("通过 node2 重置共享数据", s == 200, f"{s} {b}")
    p = pool(BASE1)
    check("node1 看到池已清空（共享持久数据）", p.get("free") == 6 and p.get("total") == 6,
          json.dumps(p, ensure_ascii=False))

    print("-- P1 基本申请（node1）")
    s, b, n = apply(BASE1, "vm-a", "req-a", 30)
    la = b.get("lease", {})
    check("申请成功(201)", s == 201, f"{s} {b}")
    check("分到池内最低空闲地址 10.99.0.1", la.get("address") == "10.99.0.1", str(la))
    check("响应带租约令牌", bool(la.get("token")), str(la))
    check("X-Served-By 为 node1", n == "node1", str(n))

    print("-- P2 同号重试（打到 node2，跨实例幂等）")
    s, b, n = apply(BASE2, "vm-a", "req-a", 30)
    lb = b.get("lease", {})
    check("重试返回 200 且 replayed=true", s == 200 and b.get("replayed") is True, f"{s} {b}")
    check("返回原租约（lease_id/地址/令牌一致）",
          lb.get("lease_id") == la.get("lease_id") and lb.get("address") == la.get("address")
          and lb.get("token") == la.get("token"), f"{la} vs {lb}")
    check("原租约状态为 active", lb.get("status") == "active", str(lb))
    check("由 node2 处理（共享库可见）", n == "node2", str(n))

    print("-- P3 同一虚拟机只能持有一个有效地址")
    s, b, _ = apply(BASE1, "vm-a", "req-a2", 30)
    check("重复申请被拒(409 vm_already_leased)",
          s == 409 and b.get("detail", {}).get("error") == "vm_already_leased", f"{s} {b}")
    check("错误响应附带已持有租约",
          b.get("detail", {}).get("existing_lease", {}).get("address") == "10.99.0.1", str(b))

    print("-- P4 填满地址池（两实例交替受理）")
    leases = {"vm-a": la}
    ok = True
    for i, vm in enumerate(["vm-b", "vm-c", "vm-d", "vm-e", "vm-f"]):
        base = BASE1 if i % 2 == 0 else BASE2
        ttl = 30 if vm == "vm-b" else 60
        s, b, _ = apply(base, vm, f"req-{vm}", ttl)
        leases[vm] = b.get("lease", {})
        ok = ok and s == 201
    addrs = [leases[v].get("address") for v in ["vm-a", "vm-b", "vm-c", "vm-d", "vm-e", "vm-f"]]
    check("5 台虚拟机全部申请成功", ok, str(addrs))
    check("6 个地址互不重复", len(set(addrs)) == 6, str(addrs))
    p = pool(BASE2)
    check("node2 视角：池已满 free=0", p.get("free") == 0 and p.get("leased") == 6,
          json.dumps(p, ensure_ascii=False))

    print("-- P5 池满返回“地址不足”")
    s, b, _ = apply(BASE1, "vm-g", "req-g", 30)
    check("返回 409 pool_exhausted", s == 409 and b.get("detail", {}).get("error") == "pool_exhausted",
          f"{s} {b}")
    check("报错文案含“地址不足”", "地址不足" in json.dumps(b, ensure_ascii=False), str(b))

    print("-- P6 续租")
    s, b, _ = req("POST", BASE1, f"/api/leases/{leases['vm-b']['lease_id']}/renew",
                  {"token": "wrong-token", "ttl_seconds": 30})
    check("错误令牌续租被拒(409 token_mismatch)",
          s == 409 and b.get("detail", {}).get("error") == "token_mismatch", f"{s} {b}")
    old_exp = leases["vm-b"]["expires_at"]
    s, b, _ = req("POST", BASE2, f"/api/leases/{leases['vm-b']['lease_id']}/renew",
                  {"token": leases["vm-b"]["token"], "ttl_seconds": 60})
    nb = b.get("lease", {})
    check("正确令牌续租成功", s == 200 and nb.get("status") == "active", f"{s} {b}")
    check("过期时间被延长", nb.get("expires_at", "") > old_exp, f"{old_exp} -> {nb.get('expires_at')}")
    leases["vm-b"] = nb

    print("-- P7 释放（幂等）")
    s, b, _ = req("POST", BASE1, f"/api/leases/{leases['vm-c']['lease_id']}/release",
                  {"token": "bad"})
    check("错误令牌释放被拒(409)", s == 409 and b.get("detail", {}).get("error") == "token_mismatch",
          f"{s} {b}")
    s, b, _ = req("POST", BASE2, f"/api/leases/{leases['vm-c']['lease_id']}/release",
                  {"token": leases["vm-c"]["token"]})
    check("释放成功，状态 released", s == 200 and b.get("lease", {}).get("status") == "released",
          f"{s} {b}")
    s, b, _ = req("POST", BASE1, f"/api/leases/{leases['vm-c']['lease_id']}/release",
                  {"token": leases["vm-c"]["token"]})
    check("重复释放幂等返回(200 无副作用)", s == 200, f"{s} {b}")
    check("释放后池有 1 个空闲地址", pool(BASE1).get("free") == 1, json.dumps(pool(BASE1)))

    print("-- P8 释放后的地址可再分配")
    s, b, _ = apply(BASE2, "vm-h", "req-h", 60)
    lh = b.get("lease", {})
    check("vm-h 申请成功", s == 201, f"{s} {b}")
    check("复用了 vm-c 释放的 10.99.0.3", lh.get("address") == "10.99.0.3", str(lh))
    leases["vm-h"] = lh

    print("-- P9 过期交错：旧号重试 / 旧令牌迟到 / 地址易主")
    req("POST", BASE1, f"/api/leases/{leases['vm-d']['lease_id']}/release",
        {"token": leases["vm-d"]["token"]})
    s, b, _ = apply(BASE1, "vm-i", "req-i", 2)
    li = b.get("lease", {})
    check("vm-i 短租约(2s)申请成功，地址 10.99.0.4", s == 201 and li.get("address") == "10.99.0.4",
          f"{s} {b}")
    time.sleep(3)
    s, b, _ = req("GET", BASE2, f"/api/leases/{li['lease_id']}")
    check("过期后状态为 expired", s == 200 and b.get("lease", {}).get("status") == "expired",
          f"{s} {b}")
    free_before = pool(BASE1).get("free")  # 过期已释放 10.99.0.4，取重试前的基准
    s, b, _ = apply(BASE2, "vm-i", "req-i", 30)
    li2 = b.get("lease", {})
    check("过期后同号重试：仍返回原租约(200 replayed)",
          s == 200 and b.get("replayed") is True and li2.get("lease_id") == li.get("lease_id"),
          f"{s} {b}")
    check("原租约当前状态为 expired，未借旧号再领地址",
          li2.get("status") == "expired" and li2.get("address") == "10.99.0.4", str(li2))
    s, b, _ = req("GET", BASE1, "/api/leases?vm_id=vm-i&status=active")
    check("vm-i 名下无有效租约", s == 200 and b.get("count") == 0, f"{s} {b}")
    check("重试未消耗空闲地址", pool(BASE1).get("free") == free_before,
          f"before={free_before} after={pool(BASE1).get('free')}")
    s, b, _ = apply(BASE2, "vm-j", "req-j", 60)
    lj = b.get("lease", {})
    check("过期地址分配给 vm-j（10.99.0.4）", s == 201 and lj.get("address") == "10.99.0.4",
          f"{s} {b}")
    s, b, _ = req("POST", BASE1, f"/api/leases/{li['lease_id']}/renew",
                  {"token": li["token"], "ttl_seconds": 30})
    check("旧令牌迟到的续租被拒(409 lease_expired)",
          s == 409 and b.get("detail", {}).get("error") == "lease_expired", f"{s} {b}")
    s, b, _ = req("POST", BASE2, f"/api/leases/{li['lease_id']}/release", {"token": li["token"]})
    check("旧令牌迟到的释放只终结旧租约(200)", s == 200, f"{s} {b}")
    s, b, _ = req("GET", BASE1, "/api/addresses/10.99.0.4")
    holder = b.get("current_holder") or {}
    check("新归属不受影响：10.99.0.4 仍归 vm-j 且 active",
          holder.get("vm_id") == "vm-j" and holder.get("status") == "active", str(b))

    print("-- P9.5 申请记录审计")
    s, b, _ = req("GET", BASE2, "/api/requests/req-a")
    results = [a.get("result") for a in b.get("attempts", [])]
    check("req-a 申请记录含 leased + replayed", "leased" in results and "replayed" in results,
          str(results))
    nodes = {a.get("node") for a in b.get("attempts", [])}
    check("申请记录可见两个实例的受理痕迹", nodes == {"node1", "node2"}, str(nodes))
    s, b, _ = req("GET", BASE1, "/api/requests/req-g")
    results = [a.get("result") for a in b.get("attempts", [])]
    check("req-g 记录为 rejected_pool_exhausted", results == ["rejected_pool_exhausted"], str(results))

    print("-- P10 双实例争抢最后一个地址")
    req("POST", BASE1, "/api/admin/reset")
    for i in range(1, 6):
        apply(BASE1, f"vm-k{i}", f"req-k{i}", 60)
    check("预备：仅剩 1 个空闲地址", pool(BASE2).get("free") == 1, json.dumps(pool(BASE2)))
    with ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(apply, BASE1, "vm-r1", "req-r1", 60)
        f2 = ex.submit(apply, BASE2, "vm-r2", "req-r2", 60)
        r1, r2 = f1.result(), f2.result()
    codes = sorted([r1[0], r2[0]])
    check("争抢结果恰好一个 201 一个 409", codes == [201, 409], f"{r1[0]} {r2[0]}")
    winner = r1 if r1[0] == 201 else r2
    check("胜者拿到最后的 10.99.0.6", winner[1].get("lease", {}).get("address") == "10.99.0.6",
          str(winner[1]))
    loser = r2 if r1[0] == 201 else r1
    check("败者收到地址不足",
          loser[1].get("detail", {}).get("error") == "pool_exhausted", str(loser[1]))
    check("争抢后池满", pool(BASE1).get("free") == 0, json.dumps(pool(BASE1)))

    print("-- P11 双实例满池争抢（12 并发抢 6 个地址）")
    req("POST", BASE2, "/api/admin/reset")

    def contender(i):
        base = BASE1 if i % 2 == 0 else BASE2
        return apply(base, f"vm-c{i:02d}", f"req-c{i:02d}", 60)

    with ThreadPoolExecutor(max_workers=12) as ex:
        results = list(ex.map(contender, range(1, 13)))
    wins = [r for r in results if r[0] == 201]
    losses = [r for r in results if r[0] == 409]
    check("恰好 6 个成功", len(wins) == 6, f"wins={len(wins)} losses={len(losses)}")
    check("恰好 6 个池满失败", len(losses) == 6 and all(
        r[1].get("detail", {}).get("error") == "pool_exhausted" for r in losses), str(losses[:1]))
    win_addrs = [r[1].get("lease", {}).get("address") for r in wins]
    check("6 个成功地址互不重复", len(set(win_addrs)) == 6, str(win_addrs))
    p = pool(BASE2)
    check("node2 视角：6 租 0 空闲", p.get("leased") == 6 and p.get("free") == 0,
          json.dumps(p, ensure_ascii=False))

    print(f"\n== 结果: {passed} 通过, {failed} 失败")
    if failed:
        sys.exit(1)
    print("== 全部通过。可接着做重启持久化核对（leases/request_log 存于共享 SQLite）。")


if __name__ == "__main__":
    main()
