#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
peer.py - Sincronizacao de diretorio P2P sobre UDP
Trabalho I - Redes de Computadores: Aplicacao e Transporte (UNISINOS)

Cada peer e, ao mesmo tempo:
  - SERVIDOR: responde LISTA (envia seu indice) e PEDIR (envia DADOS do arquivo);
  - CLIENTE : detecta mudancas no diretorio local, anuncia (ANUNCIO/REMOVIDO)
              e baixa dos outros os arquivos que ainda nao tem.

Uso:
  python peer.py A --config peers.txt --dir tmp
"""

import argparse
import base64
import hashlib
import json
import math
import os
import random
import socket
import sys
import threading
import time
import uuid

# ---------------------------------------------------------------- parametros
CHUNK_SIZE = 4096          # bytes por pedaco (cabe em qualquer datagrama, inclusive no macOS)
WINDOW = 256               # quantos pedacos pedir por vez (1 MB)
HEARTBEAT_INTERVAL = 2.0   # PING para todos a cada 2 s
PEER_TIMEOUT = 6.0         # sem noticias por 6 s => peer inativo
SCAN_INTERVAL = 1.0        # varredura do diretorio local
SYNC_INTERVAL = 5.0        # sincronizacao periodica (anti-entropia) via LISTA
RESEND_INTERVAL = 1.0      # reenvio de ANUNCIO/REMOVIDO sem ACK
MAX_RESENDS = 10
REQUEST_TIMEOUT = 0.4      # pedacos que nao chegaram => pedir de novo
MAX_REQUEST_RETRIES = 8    # depois disso, tenta outra fonte
LISTA_PAGE = 25            # entradas do indice por datagrama de LISTA_RESP
SEEN_TTL = 120.0


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()


def load_config(path):
    """Arquivo com uma linha por peer: ID HOST PORTA"""
    peers = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.split("#", 1)[0].strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) != 3:
                raise ValueError(f"Linha invalida no config: {line!r} (esperado: ID HOST PORTA)")
            pid, host, port = parts
            peers[pid] = (host, int(port))
    return peers


def safe_name(name):
    """Aceita apenas nomes simples (sem caminho) e ignora arquivos ocultos/temporarios."""
    return (isinstance(name, str) and name != "" and name == os.path.basename(name)
            and not name.startswith(".") and "/" not in name and "\\" not in name)


def version_key(entry):
    """Ultima escrita vence (timestamp); desempate pelo hash."""
    return (entry["ts"], entry["hash"])


class Peer:
    def __init__(self, my_id, peers, directory, port=None, loss=0.0, status_interval=0):
        if my_id not in peers and port is None:
            raise ValueError(f"Peer {my_id} nao esta no config; informe --porta")
        self.id = my_id
        self.port = port or peers[my_id][1]
        self.dir = os.path.abspath(directory)
        os.makedirs(self.dir, exist_ok=True)
        self.loss = loss
        self.status_interval = status_interval

        self.lock = threading.RLock()
        self.print_lock = threading.Lock()
        self.running = True

        self.peers = {pid: addr for pid, addr in peers.items() if pid != my_id}
        self.resolved = {}       # id -> ((ip, porta) | None, instante)
        self.peer_info = {pid: {"last": 0.0, "files": None, "total": 0} for pid in self.peers}

        self.index = {}          # nome -> {hash, tamanho, ts, removido, mtime}
        self.pending = {}        # mid -> mensagem aguardando ACK
        self.seen = {}           # mids ja processados (descarta duplicados)
        self.downloads = {}      # nome -> estado do download

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        except OSError:
            pass
        if os.name == "nt":  # evita ConnectionResetError no Windows quando um peer esta offline
            try:
                self.sock.ioctl(socket.SIO_UDP_CONNRESET, False)
            except (AttributeError, OSError):
                pass
        self.sock.bind(("0.0.0.0", self.port))
        self.sock.settimeout(0.5)

    # ------------------------------------------------------------ utilidades
    def log(self, text):
        with self.print_lock:
            print(f"[{time.strftime('%H:%M:%S')}] [{self.id}] {text}", flush=True)

    def resolve(self, pid):
        host, port = self.peers[pid]
        now = time.time()
        cached = self.resolved.get(pid)
        if cached and now - cached[1] < (30 if cached[0] else 5):
            return cached[0]
        try:
            addr = (socket.gethostbyname(host), port)
        except OSError:
            addr = None
        self.resolved[pid] = (addr, now)
        return addr

    def send(self, pid, msg):
        addr = self.resolve(pid)
        if addr is None:
            return
        if self.loss and random.random() < self.loss:
            return  # perda simulada (--perda) para demonstrar a tolerancia do protocolo
        msg = dict(msg)
        msg["from"] = self.id
        try:
            self.sock.sendto(json.dumps(msg, separators=(",", ":")).encode("utf-8"), addr)
        except OSError:
            pass

    def alive(self, pid, now=None):
        now = now or time.time()
        return now - self.peer_info[pid]["last"] < PEER_TIMEOUT

    def live_files(self):
        return sorted(n for n, e in self.index.items() if not e["removido"])

    def wire(self, entry):
        return {k: entry[k] for k in ("hash", "tamanho", "ts", "removido")}

    # ------------------------------------------------- envio confiavel (ACK)
    def broadcast_reliable(self, msg):
        msg = dict(msg)
        msg["mid"] = uuid.uuid4().hex[:12]
        with self.lock:
            self.pending[msg["mid"]] = {"msg": msg, "targets": set(self.peers),
                                        "tries": 1, "last": time.time()}
            targets = list(self.peers)
        for pid in targets:
            self.send(pid, msg)

    def announce(self, name):
        e = self.index[name]
        if e["removido"]:
            self.broadcast_reliable({"tipo": "REMOVIDO", "nome": name, "ts": e["ts"]})
        else:
            self.broadcast_reliable({"tipo": "ANUNCIO", "nome": name, "hash": e["hash"],
                                     "tamanho": e["tamanho"], "ts": e["ts"]})

    # ------------------------------------------------- varredura do diretorio
    def scan(self, initial=False):
        with self.lock:
            try:
                entries = list(os.scandir(self.dir))
            except OSError:
                return
            current = {}
            for en in entries:
                if not safe_name(en.name):
                    continue
                try:
                    if en.is_file():
                        current[en.name] = en.stat()
                except OSError:
                    pass

            # arquivos novos ou alterados
            for name, st in current.items():
                if name in self.downloads:
                    continue
                e = self.index.get(name)
                if e and not e["removido"] and e["tamanho"] == st.st_size and e.get("mtime") == st.st_mtime:
                    continue
                try:
                    h = sha256_file(os.path.join(self.dir, name))
                except OSError:
                    continue  # arquivo ainda sendo copiado/bloqueado; tenta na proxima volta
                if e and not e["removido"] and e["hash"] == h:
                    e["mtime"] = st.st_mtime
                    continue
                ts = st.st_mtime if initial else time.time()
                if e and ts <= e["ts"]:
                    ts = e["ts"] + 0.001
                self.index[name] = {"hash": h, "tamanho": st.st_size, "ts": ts,
                                    "removido": False, "mtime": st.st_mtime}
                if initial:
                    self.log(f"[+] {name} ({st.st_size} bytes) ja estava no diretorio")
                else:
                    self.log(f"[+] {name} ({st.st_size} bytes) adicionado localmente -> ANUNCIO")
                    self.announce(name)

            # arquivos apagados
            for name, e in list(self.index.items()):
                if not e["removido"] and name not in current and name not in self.downloads:
                    e.update(removido=True, hash="", tamanho=0, mtime=None,
                             ts=max(time.time(), e["ts"] + 0.001))
                    self.log(f"[-] {name} apagado localmente -> REMOVIDO")
                    self.announce(name)

    # --------------------------------------- decisao: aplicar versao remota?
    def consider(self, name, remote, source):
        if not safe_name(name):
            return
        remote = {"hash": remote.get("hash", ""), "tamanho": int(remote.get("tamanho", 0)),
                  "ts": float(remote["ts"]), "removido": bool(remote.get("removido"))}
        with self.lock:
            local = self.index.get(name)
            dl = self.downloads.get(name)
            current = local
            if dl and (local is None or version_key(dl["entry"]) > version_key(local)):
                current = dl["entry"]
            if current and version_key(remote) <= version_key(current):
                return  # ja temos esta versao (ou uma mais nova)

            if remote["removido"]:
                self.downloads.pop(name, None)
                existed = local is not None and not local["removido"]
                try:
                    os.remove(os.path.join(self.dir, name))
                except FileNotFoundError:
                    pass
                except OSError as ex:
                    self.log(f"nao consegui apagar {name}: {ex} (tento de novo depois)")
                    return
                self.index[name] = dict(remote, mtime=None)
                if existed:
                    self.log(f"[-] {name} removido (aviso de {source})")
                return

            if local and not local["removido"] and local["hash"] == remote["hash"]:
                local["ts"] = remote["ts"]  # mesmo conteudo, so adota o timestamp
                return
            if dl and dl["entry"]["hash"] == remote["hash"]:
                dl["entry"] = remote
                if source not in dl["sources"]:
                    dl["sources"].append(source)
                return
            self.start_download(name, remote, source)

    # ---------------------------------------------------------- cliente: baixar
    def start_download(self, name, entry, source):
        total = math.ceil(entry["tamanho"] / CHUNK_SIZE)
        if total == 0:
            self.finish_download(name, entry, b"", source)
            return
        self.downloads[name] = {"entry": entry, "sources": [source], "src_idx": 0, "total": total,
                                "parts": {}, "requested": [], "last_req": 0.0, "retries": 0}
        self.log(f"[v] baixando {name} ({entry['tamanho']} bytes, {total} partes) de {source}")
        self.request_parts(name)

    def request_parts(self, name):
        dl = self.downloads[name]
        missing = [i for i in range(dl["total"]) if i not in dl["parts"]][:WINDOW]
        src = dl["sources"][dl["src_idx"] % len(dl["sources"])]
        dl["requested"] = missing
        dl["last_req"] = time.time()
        self.send(src, {"tipo": "PEDIR", "nome": name, "hash": dl["entry"]["hash"], "partes": missing})

    def on_dados(self, msg, sender):
        name = msg.get("nome")
        with self.lock:
            dl = self.downloads.get(name)
            if not dl or dl["entry"]["hash"] != msg.get("hash"):
                return
            part = int(msg.get("parte", -1))
            if not 0 <= part < dl["total"] or part in dl["parts"]:
                return  # fora do intervalo ou duplicado
            dl["parts"][part] = base64.b64decode(msg["dados"])
            dl["retries"] = 0
            if len(dl["parts"]) == dl["total"]:
                data = b"".join(dl["parts"][i] for i in range(dl["total"]))
                del self.downloads[name]
                self.finish_download(name, dl["entry"], data, sender)
            elif all(p in dl["parts"] for p in dl["requested"]):
                self.request_parts(name)  # janela completa: pede a proxima

    def finish_download(self, name, entry, data, source):
        if hashlib.sha256(data).hexdigest() != entry["hash"]:
            self.log(f"[!] {name}: hash nao confere, descartado (sera pedido de novo)")
            return
        final = os.path.join(self.dir, name)
        temp = os.path.join(self.dir, f".{name}.part")
        try:
            with open(temp, "wb") as f:
                f.write(data)
            os.replace(temp, final)
            mtime = os.stat(final).st_mtime
        except OSError as ex:
            self.log(f"[!] nao consegui gravar {name}: {ex}")
            return
        self.index[name] = dict(entry, mtime=mtime)
        self.log(f"[v] {name} recebido de {source} ({len(data)} bytes)")

    # ------------------------------------------------------- servidor: enviar
    def on_pedir(self, msg, sender):
        name = msg.get("nome")
        with self.lock:
            e = self.index.get(name)
            ok = e is not None and not e["removido"] and e["hash"] == msg.get("hash")
        if not ok or not safe_name(name):
            return
        try:
            with open(os.path.join(self.dir, name), "rb") as f:
                for p in list(msg.get("partes", []))[:WINDOW]:
                    if not isinstance(p, int) or p < 0:
                        continue
                    f.seek(p * CHUNK_SIZE)
                    chunk = f.read(CHUNK_SIZE)
                    self.send(sender, {"tipo": "DADOS", "nome": name, "hash": e["hash"], "parte": p,
                                       "dados": base64.b64encode(chunk).decode("ascii")})
                    time.sleep(0.0002)
        except OSError:
            pass

    def on_lista(self, sender):
        with self.lock:
            items = [(n, self.wire(e)) for n, e in self.index.items()]
        if not items:
            self.send(sender, {"tipo": "LISTA_RESP", "indice": {}})
        for i in range(0, len(items), LISTA_PAGE):
            self.send(sender, {"tipo": "LISTA_RESP", "indice": dict(items[i:i + LISTA_PAGE])})

    # ---------------------------------------------------- recepcao/despacho
    def receiver_loop(self):
        while self.running:
            try:
                data, addr = self.sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                continue
            try:
                msg = json.loads(data.decode("utf-8"))
                self.handle(msg, addr)
            except Exception as ex:  # mensagem malformada nao derruba o peer
                self.log(f"[!] erro ao tratar mensagem de {addr}: {ex}")

    def handle(self, msg, addr):
        sender, tipo = msg.get("from"), msg.get("tipo")
        if not sender or sender == self.id:
            return
        now = time.time()
        with self.lock:
            if sender not in self.peers:  # peer novo que nos conhece: passa a fazer parte da rede
                self.peers[sender] = addr
                self.peer_info[sender] = {"last": 0.0, "files": None, "total": 0}
                self.log(f"[*] novo peer {sender} em {addr[0]}:{addr[1]}")
            info = self.peer_info[sender]
            was_alive = now - info["last"] < PEER_TIMEOUT
            info["last"] = now
        if not was_alive:
            self.log(f"[*] peer {sender} ativo -> pedindo LISTA")
            self.send(sender, {"tipo": "LISTA"})

        mid = msg.get("mid")
        if mid:
            self.send(sender, {"tipo": "ACK", "ref": mid})
            with self.lock:
                if mid in self.seen:
                    return  # duplicado
                self.seen[mid] = now

        if tipo == "ACK":
            with self.lock:
                p = self.pending.get(msg.get("ref"))
                if p:
                    p["targets"].discard(sender)
                    if not p["targets"]:
                        del self.pending[msg["ref"]]
        elif tipo == "PING":
            with self.lock:
                info["files"] = msg.get("arquivos", [])
                info["total"] = msg.get("total", len(info["files"]))
        elif tipo == "ANUNCIO":
            self.consider(msg["nome"], {"hash": msg["hash"], "tamanho": msg["tamanho"],
                                        "ts": msg["ts"], "removido": False}, sender)
        elif tipo == "REMOVIDO":
            self.consider(msg["nome"], {"hash": "", "tamanho": 0, "ts": msg["ts"], "removido": True}, sender)
        elif tipo == "LISTA":
            self.on_lista(sender)
        elif tipo == "LISTA_RESP":
            for name, entry in msg.get("indice", {}).items():
                self.consider(name, entry, sender)
        elif tipo == "PEDIR":  # servidor atende em thread propria para nao travar a recepcao
            threading.Thread(target=self.on_pedir, args=(msg, sender), daemon=True).start()
        elif tipo == "DADOS":
            self.on_dados(msg, sender)

    # ------------------------------------------------------- tarefas periodicas
    def scanner_loop(self):
        while self.running:
            time.sleep(SCAN_INTERVAL)
            self.scan()

    def heartbeat_loop(self):
        while self.running:
            with self.lock:
                files = self.live_files()
                targets = list(self.peers)
            shown, size = [], 0
            for f in files:  # limita o tamanho do datagrama
                size += len(f) + 4
                if size > 3000:
                    break
                shown.append(f)
            for pid in targets:
                self.send(pid, {"tipo": "PING", "arquivos": shown, "total": len(files)})
            time.sleep(HEARTBEAT_INTERVAL)

    def maintenance_loop(self):
        last_sync = time.time()
        while self.running:
            time.sleep(0.2)
            now = time.time()
            resend, rerequest = [], []
            with self.lock:
                # reenvio de ANUNCIO/REMOVIDO que ainda nao tiveram ACK
                for mid, p in list(self.pending.items()):
                    if now - p["last"] >= RESEND_INTERVAL:
                        if p["tries"] >= MAX_RESENDS:
                            del self.pending[mid]  # a sincronizacao periodica cobre o resto
                            continue
                        p["tries"] += 1
                        p["last"] = now
                        resend += [(t, p["msg"]) for t in p["targets"]]
                # pedacos que nao chegaram
                for name, dl in list(self.downloads.items()):
                    if now - dl["last_req"] > REQUEST_TIMEOUT:
                        dl["retries"] += 1
                        if dl["retries"] > MAX_REQUEST_RETRIES:
                            dl["retries"] = 0
                            dl["src_idx"] += 1
                            if dl["src_idx"] >= 2 * len(dl["sources"]):
                                del self.downloads[name]
                                self.log(f"[!] download de {name} falhou; tento na proxima sincronizacao")
                                continue
                        rerequest.append(name)
                for name in rerequest:
                    self.request_parts(name)
                for mid, t in list(self.seen.items()):
                    if now - t > SEEN_TTL:
                        del self.seen[mid]
                alive_peers = [pid for pid in self.peers if self.alive(pid, now)]
            for pid, msg in resend:
                self.send(pid, msg)
            if now - last_sync >= SYNC_INTERVAL:  # anti-entropia: cobre qualquer mensagem perdida
                last_sync = now
                for pid in alive_peers:
                    self.send(pid, {"tipo": "LISTA"})

    def status_loop(self):
        while self.running and self.status_interval > 0:
            time.sleep(self.status_interval)
            self.print_status()

    # --------------------------------------------------- visao sumarizada
    def print_status(self):
        now = time.time()
        with self.lock:
            mine = self.live_files()
            rows = [(f"{self.id} (eu)", "ATIVO", "-", len(mine), mine)]
            in_sync = True
            for pid in sorted(self.peers):
                info = self.peer_info[pid]
                if self.alive(pid, now):
                    files = info["files"] or []
                    rows.append((pid, "ATIVO", f"{now - info['last']:.0f}s", info["total"], files))
                    if info["total"] != len(mine) or sorted(files) != mine[:len(files)]:
                        in_sync = False
                else:
                    seen = "nunca" if info["last"] == 0 else f"{now - info['last']:.0f}s"
                    rows.append((pid, "inativo", seen, None, []))
            dls = [(n, len(d["parts"]) * 100 // d["total"]) for n, d in self.downloads.items()]
        out = ["", "=" * 70, f" ESTADO DA REDE - visto pelo peer {self.id}  ({time.strftime('%H:%M:%S')})",
               "=" * 70, f" {'PEER':<10}{'STATUS':<10}{'ULT.MSG':<9}{'QTD':<5}ARQUIVOS", "-" * 70]
        for pid, st, seen, qtd, files in rows:
            lst = ", ".join(files) if files else "-"
            if qtd is not None and qtd > len(files):
                lst += ", ..."
            if len(lst) > 36:
                lst = lst[:33] + "..."
            out.append(f" {pid:<10}{st:<10}{seen:<9}{'-' if qtd is None else qtd:<5}{lst}")
        out.append("-" * 70)
        if dls:
            out.append(" Downloads: " + ", ".join(f"{n} {p}%" for n, p in dls))
        out.append(" Rede SINCRONIZADA" if in_sync and not dls else " Sincronizando...")
        out.append("=" * 70)
        with self.print_lock:
            print("\n".join(out), flush=True)

    def print_list(self):
        with self.lock:
            items = sorted(self.index.items())
        with self.print_lock:
            print(f"\n Arquivos na rede (indice do peer {self.id}):")
            live = [(n, e) for n, e in items if not e["removido"]]
            for n, e in live:
                print(f"   {n:<35}{e['tamanho']:>12} bytes  {e['hash'][:12]}")
            if not live:
                print("   (nenhum)")
            dead = [n for n, e in items if e["removido"]]
            if dead:
                print(f" Removidos (lapides): {', '.join(dead)}")
            print(flush=True)

    # --------------------------------------------------------------- main
    def start(self):
        self.log(f"iniciando na porta UDP {self.port}, diretorio {self.dir}")
        self.log("peers conhecidos: " + ", ".join(f"{p}={h}:{pt}" for p, (h, pt) in self.peers.items()))
        if self.loss:
            self.log(f"perda simulada de {self.loss:.0%} dos datagramas enviados")
        self.scan(initial=True)
        for target in (self.receiver_loop, self.scanner_loop, self.heartbeat_loop,
                       self.maintenance_loop, self.status_loop):
            threading.Thread(target=target, daemon=True).start()
        for pid in list(self.peers):
            self.send(pid, {"tipo": "LISTA"})

    def stop(self):
        self.running = False


def main():
    ap = argparse.ArgumentParser(description="Peer de sincronizacao de diretorio sobre UDP")
    ap.add_argument("id", help="identificador deste peer (ex.: A)")
    ap.add_argument("--config", default="peers.txt", help="arquivo com ID HOST PORTA por linha")
    ap.add_argument("--dir", default="tmp", help="diretorio sincronizado (padrao: tmp)")
    ap.add_argument("--porta", type=int, help="porta local (padrao: a do config)")
    ap.add_argument("--status", type=float, default=0, help="mostrar o estado a cada N segundos")
    ap.add_argument("--perda", type=float, default=0.0, help="perda simulada de datagramas (0 a 1)")
    args = ap.parse_args()

    peer = Peer(args.id, load_config(args.config), args.dir, args.porta, args.perda, args.status)
    peer.start()
    print("Comandos: status | lista | sync | sair", flush=True)
    try:
        while True:
            try:
                cmd = input().strip().lower()
            except EOFError:  # sem terminal (ex.: container em segundo plano)
                while True:
                    time.sleep(3600)
            if cmd in ("status", "s"):
                peer.print_status()
            elif cmd in ("lista", "l"):
                peer.print_list()
            elif cmd == "sync":
                for pid in list(peer.peers):
                    peer.send(pid, {"tipo": "LISTA"})
                peer.log("LISTA enviada a todos")
            elif cmd in ("sair", "exit", "quit", "q"):
                break
            elif cmd:
                print("Comandos: status | lista | sync | sair", flush=True)
    except KeyboardInterrupt:
        pass
    peer.stop()
    peer.log("encerrado")


if __name__ == "__main__":
    main()
