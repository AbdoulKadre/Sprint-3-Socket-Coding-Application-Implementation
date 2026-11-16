#!/usr/bin/env python3
"""
server.py -- TTT-1 Tic-Tac-Toe game server (CS 457, Sprint 3).

Usage:
    python3 server.py -h
    python3 server.py -p 8080
"""

import argparse
import json
import logging
import socket
import sys
import threading
import time


MAX_FRAME_BYTES = 4096          
DELIMITER = b"\n"               
RECV_CHUNK = 4096               

# The 8 message types in the inventory
MSG_CONNECT = "CONNECT"
MSG_LOBBY_WAIT = "LOBBY_WAIT"
MSG_GAME_START = "GAME_START"
MSG_MOVE = "MOVE"
MSG_STATE_UPDATE = "STATE_UPDATE"
MSG_ERROR = "ERROR"
MSG_DISCONNECT = "DISCONNECT"
MSG_GAME_OVER = "GAME_OVER"

MESSAGE_TYPES = (
    MSG_CONNECT, MSG_LOBBY_WAIT, MSG_GAME_START, MSG_MOVE,
    MSG_STATE_UPDATE, MSG_ERROR, MSG_DISCONNECT, MSG_GAME_OVER,
)
CLIENT_TO_SERVER = (MSG_CONNECT, MSG_MOVE, MSG_DISCONNECT)

# Error codes (section 6.6).
E_MALFORMED_JSON = "MALFORMED_JSON"
E_MALFORMED_MESSAGE = "MALFORMED_MESSAGE"
E_UNKNOWN_TYPE = "UNKNOWN_TYPE"
E_OUT_OF_TURN = "OUT_OF_TURN"
E_INVALID_MOVE = "INVALID_MOVE"
E_NOT_CONNECTED = "NOT_CONNECTED"
E_FRAME_TOO_LONG = "FRAME_TOO_LONG"
E_ROOM_FULL = "ROOM_FULL"

MAX_ALIAS_LEN = 16
MAX_DETAIL_LEN = 200



#Exceptions
class ProtocolError(Exception):
    """Base class for TTT-1 protocol violations."""


class FrameTooLongError(ProtocolError):
    """A frame exceeded MAX_FRAME_BYTES (section 3.1); connection closes."""


class MalformedFrameError(ProtocolError):
    """A complete frame was not valid UTF-8 JSON (section 9).

    The connection stays open (single-strike policy).
    """

# Encoding (section 3.1 / ai_prompts.md task 3.1)
def encode_message(msg_type, player_id, payload, timestamp=None):
    if timestamp is None:
        timestamp = int(time.time())
    envelope = {
        "msg_type": msg_type,
        "player_id": player_id,
        "payload": payload,
        "timestamp": timestamp,
    }
    text = json.dumps(envelope, separators=(",", ":"))
    frame = text.encode("utf-8") + DELIMITER
    if len(frame) > MAX_FRAME_BYTES:
        raise FrameTooLongError(f"outbound frame is {len(frame)} bytes")
    return frame


def make_message(msg_type, player_id, payload):
    return {
        "msg_type": msg_type,
        "player_id": player_id,
        "payload": payload,
        "timestamp": int(time.time()),
    }


def make_error(player_id, code, detail):
    payload = {"code": code, "detail": detail[:MAX_DETAIL_LEN]}
    return make_message(MSG_ERROR, player_id, payload)


def send_message(sock, message):
    sock.sendall(encode_message(message["msg_type"], message["player_id"],
                                message["payload"], message["timestamp"]))

# Buffered frame decoder
class FrameReader:
    def __init__(self, sock):
        self.sock = sock
        self.buf = b""

    def read_frame(self):
        while True:
            idx = self.buf.find(DELIMITER)
            if idx != -1:
                frame, self.buf = self.buf[:idx], self.buf[idx + 1:]
                if len(frame) + 1 > MAX_FRAME_BYTES:
                    raise FrameTooLongError(f"frame of {len(frame) + 1} bytes")
                return self._parse(frame)
            
            if len(self.buf) >= MAX_FRAME_BYTES:
                raise FrameTooLongError("no newline within 4096 bytes")

            chunk = self.sock.recv(RECV_CHUNK)
            if not chunk:            
                return None
            self.buf += chunk

    @staticmethod
    def _parse(frame):
        try:
            return json.loads(frame.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MalformedFrameError(str(exc)) from exc

# Schema validation (section 4 envelope + section 6 payloads)
def _is_int(value):
    """True for real integers (bool is a subclass of int, so exclude it)."""
    return isinstance(value, int) and not isinstance(value, bool)


def is_valid_alias(alias):
    return (isinstance(alias, str)
            and 1 <= len(alias) <= MAX_ALIAS_LEN
            and all(0x20 <= ord(ch) <= 0x7E for ch in alias))


def validate_envelope(msg):
    if not isinstance(msg, dict):
        return E_MALFORMED_MESSAGE, 
    for field in ("msg_type", "player_id", "payload", "timestamp"):
        if field not in msg:
            return E_MALFORMED_MESSAGE, f"Missing required field '{field}'."
    if not isinstance(msg["msg_type"], str):
        return E_MALFORMED_MESSAGE, "'msg_type' must be a string."
    if msg["msg_type"] not in MESSAGE_TYPES:
        return E_UNKNOWN_TYPE, f"Unknown msg_type '{msg['msg_type']}'."
    if not is_valid_alias(msg["player_id"]):
        return (E_MALFORMED_MESSAGE,
                "'player_id' must be 1-16 printable ASCII chars.")
    if not isinstance(msg["payload"], dict):
        return E_MALFORMED_MESSAGE, "'payload' must be an object."
    if not _is_int(msg["timestamp"]) or msg["timestamp"] < 0:
        return (E_MALFORMED_MESSAGE,
                "'timestamp' must be a non-negative integer.")
    return None


def validate_move_payload(payload):
    for field in ("row", "col"):
        if field not in payload:
            return E_MALFORMED_MESSAGE, f"MOVE payload missing '{field}'."
        if not _is_int(payload[field]):
            return E_MALFORMED_MESSAGE, f"MOVE '{field}' must be an integer."
    return None

# Board helpers
EMPTY = "."


def new_board():
    return [EMPTY * 3 for _ in range(3)]


def render_board(board):
    lines = ["     0   1   2"]
    for r, row in enumerate(board):
        cells = [" " if ch == EMPTY else ch for ch in row]
        lines.append(f"  {r}  " + " | ".join(cells))
        if r < 2:
            lines.append("    ---+---+---")
    return "\n".join(lines)

INIT = "INIT"
WAITING_FOR_PLAYERS = "WAITING_FOR_PLAYERS"
GAME_START = "GAME_START"
PLAYER_TURN = "PLAYER_TURN"
EVALUATE_MOVE = "EVALUATE_MOVE"
GAME_OVER = "GAME_OVER"

#FSM events 
EV_SOCKET_BOUND = "SOCKET_BOUND"
EV_CONNECT = "CONNECT"
EV_MOVE = "MOVE"
EV_DISCONNECT = "DISCONNECT"
EV_CLIENT_DISCONNECTED = "CLIENT_DISCONNECTED"

# The 8 winning lines 
WIN_LINES = (
    [((r, 0), (r, 1), (r, 2)) for r in range(3)]
    + [((0, c), (1, c), (2, c)) for c in range(3)]
    + [((0, 0), (1, 1), (2, 2)), ((0, 2), (1, 1), (2, 0))]
)

# player_id used when replying to a client that never sent CONNECT.
UNASSIGNED_ID = "UNASSIGNED"

CLIENT_TIMEOUT_SEC = 30

log = logging.getLogger("ttt-server")


class GameFSM:
    def __init__(self):
        self.state = INIT
        self.players = []        
        self.board = new_board()
        self.active = None       
        self.scores = {}

    #helpers
    def _transition(self, new_state, reason):
        if new_state != self.state:
            log.info("FSM %s -> %s (%s)", self.state, new_state, reason)
        self.state = new_state

    def player_for(self, conn):
        for pl in self.players:
            if pl["conn"] is conn:
                return pl
        return None

    def _opponent(self, pl):
        return next(o for o in self.players if o is not pl)

    def _unique_alias(self, alias):
        taken = {pl["id"] for pl in self.players}
        if alias not in taken:
            return alias
        candidate = alias[: MAX_ALIAS_LEN - 2] + "_2"
        return candidate if candidate not in taken else "Player_2"

    def _winner_mark(self):
        for line in WIN_LINES:
            marks = {self.board[r][c] for r, c in line}
            if len(marks) == 1 and EMPTY not in marks:
                return marks.pop()
        return None

    def _board_full(self):
        return all(EMPTY not in row for row in self.board)

    def _game_over_to(self, recipients, outcome, winner, reason):
        payload = {
            "outcome": outcome,
            "winner": winner,
            "reason": reason,
            "final_board": list(self.board),
            "final_scores": dict(self.scores),
        }
        return [(pl["conn"], make_message(MSG_GAME_OVER, pl["id"], payload))
                for pl in recipients]

    def _cleanup(self):
        conns = [pl["conn"] for pl in self.players]
        self.players = []
        self.board = new_board()
        self.active = None
        self.scores = {}
        self._transition(WAITING_FOR_PLAYERS,
                         "room cleared, ready for a new game")
        return conns

    # -- main dispatcher ---------------------------------------------------
    def handle(self, event, conn=None, ctx=None):
        ctx = ctx or {}
        if event == EV_SOCKET_BOUND:
            if self.state == INIT:
                self._transition(WAITING_FOR_PLAYERS,
                                 "socket bound, accepting clients")
            return [], []
        if event == EV_CONNECT:
            return self._on_connect(conn, ctx)
        if event == EV_MOVE:
            return self._on_move(conn, ctx)
        if event in (EV_DISCONNECT, EV_CLIENT_DISCONNECTED):
            return self._on_disconnect(conn, event)
        raise ValueError(f"unknown FSM event {event!r}")

    def _on_connect(self, conn, ctx):
        alias = ctx["player_id"]

        # A second CONNECT on an already-seated connection is a sequence
        # error.
        seated = self.player_for(conn)
        if seated:
            return [(conn, make_error(seated["id"], E_MALFORMED_MESSAGE,
                                        "Already connected."))], []

        # Room full: a game is running or two players are already seated.
        if self.state != WAITING_FOR_PLAYERS or len(self.players) >= 2:
            log.info("Rejecting '%s': room full", alias)
            error = make_error(alias, E_ROOM_FULL,
                                 "A game is already in progress.")
            return [(conn, error)], [conn]

        alias = self._unique_alias(alias)

        # 1st valid CONNECT: seat as X, send LOBBY_WAIT, and stay in
        # WAITING_FOR_PLAYERS.
        if not self.players:
            pl = {"conn": conn, "id": alias, "role": "X"}
            self.players.append(pl)
            log.info("Player 1 '%s' joined as X; waiting for Player 2", alias)
            return [(conn, make_message(MSG_LOBBY_WAIT, alias,
                                          {"role": "X", "position": 1}))], []

        # 2nd valid CONNECT: seat as O -> GAME_START -> (entry action)
        # -> PLAYER_TURN.
        pl = {"conn": conn, "id": alias, "role": "O"}
        self.players.append(pl)
        log.info("Player 2 '%s' joined as O", alias)
        self._transition(GAME_START, "two players connected, roles assigned")

        self.board = new_board()
        self.scores = {x["id"]: 0 for x in self.players}
        self.active = self.players[0]       
        out = []
        for me in self.players:
            payload = {
                "role": me["role"],
                "opponent_id": self._opponent(me)["id"],
                "board": list(self.board),
                "first_turn": self.active["id"],
            }
            msg = make_message(MSG_GAME_START, me["id"], payload)
            out.append((me["conn"], msg))
        self._transition(PLAYER_TURN,
                         f"GAME_START sent, {self.active['id']} (X) to move")
        return out, []

    def _on_move(self, conn, ctx):
        pl = self.player_for(conn)
        if pl is None:
            return [(conn, make_error(UNASSIGNED_ID, E_NOT_CONNECTED,
                                        "Send CONNECT before MOVE."))], []
        if self.state != PLAYER_TURN:
            return [(conn, make_error(pl["id"], E_OUT_OF_TURN,
                                        "The game has not started yet."))], []

        # Guard 1: only the active player may move.
        # The state does not change.
        if pl is not self.active:
            error = make_error(pl["id"], E_OUT_OF_TURN,
                                 f"It is {self.active['id']}'s turn.")
            return [(conn, error)], []

        # Guard 2: coordinates in range and cell empty.
        # On failure the same player retries.
        row, col = ctx["row"], ctx["col"]
        if not (0 <= row <= 2 and 0 <= col <= 2):
            error = make_error(pl["id"], E_INVALID_MOVE,
                                 "Row and col must each be 0, 1, or 2.")
            return [(conn, error)], []
        if self.board[row][col] != EMPTY:
            error = make_error(pl["id"], E_INVALID_MOVE,
                                 f"Cell ({row},{col}) is already taken.")
            return [(conn, error)], []

        # Valid move: apply it and evaluate.
        self._transition(EVALUATE_MOVE, f"{pl['id']} played ({row},{col})")
        r = self.board[row]
        self.board[row] = r[:col] + pl["role"] + r[col + 1:]

        winner_mark = self._winner_mark()
        if winner_mark:
            self.scores[pl["id"]] += 1
            self._transition(GAME_OVER, f"{pl['id']} completed a line")
            out = self._game_over_to(self.players, "WIN", pl["id"],
                                     "LINE_COMPLETE")
            return out, self._cleanup()
        if self._board_full():
            self._transition(GAME_OVER, "board full, draw")
            out = self._game_over_to(self.players, "DRAW", None, "BOARD_FULL")
            return out, self._cleanup()

        self.active = self._opponent(pl)
        self._transition(PLAYER_TURN, f"{self.active['id']} to move")
        out = []
        for x in self.players:
            payload = {"board": list(self.board),
                       "active_turn": self.active["id"],
                       "scores": dict(self.scores)}
            msg = make_message(MSG_STATE_UPDATE, x["id"], payload)
            out.append((x["conn"], msg))
        return out, []

    def _on_disconnect(self, conn, event):
        pl = self.player_for(conn)
        if pl is None:
            return [], [conn]    

        if self.state == WAITING_FOR_PLAYERS:
            self.players.remove(pl)
            log.info("'%s' left the lobby (%s)", pl["id"], event)
            return [], [conn]
        opp = self._opponent(pl)
        self.scores[opp["id"]] += 1
        self._transition(GAME_OVER,
                         f"{pl['id']} disconnected ({event}); "
                         f"{opp['id']} wins by forfeit")
        out = self._game_over_to([opp], "FORFEIT", opp["id"], "FORFEIT")
        return out, self._cleanup()


class Connection:
    def __init__(self, sock, addr):
        self.sock = sock
        self.addr = addr
        self.reader = FrameReader(sock)
        self.closed = False

    def __repr__(self):
        return f"{self.addr[0]}:{self.addr[1]}"


class GameRoom:
    def __init__(self):
        self.lock = threading.Lock()
        self.fsm = GameFSM()

    def dispatch(self, event, conn, ctx=None):
        with self.lock:
            outbound, to_close = self.fsm.handle(event, conn, ctx)
            for target, msg in outbound:
                safe_send(target, msg)
            for target in to_close:
                close_conn(target)

    def is_active_player(self, conn):
        with self.lock:
            pl = self.fsm.player_for(conn)
            return (pl is not None and self.fsm.state == PLAYER_TURN
                    and self.fsm.active is pl)

    def send_error(self, conn, code, detail):
        with self.lock:
            pl = self.fsm.player_for(conn)
            player_id = pl["id"] if pl else UNASSIGNED_ID
            safe_send(conn, make_error(player_id, code, detail))


def safe_send(conn, msg):
    if conn.closed:
        return
    try:
        send_message(conn.sock, msg)
        if msg["msg_type"] == MSG_ERROR:
            log.info("-> %s ERROR %s", conn, msg["payload"]["code"])
        else:
            log.debug("-> %s %s", conn, msg["msg_type"])
    except OSError as exc:
        log.warning("Send to %s failed: %s", conn, exc)


def close_conn(conn):
    if conn.closed:
        return
    conn.closed = True
    try:
        conn.sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    conn.sock.close()
    log.info("Closed connection %s", conn)


def serve_client(conn, room):
    conn.sock.settimeout(CLIENT_TIMEOUT_SEC)
    try:
        while not conn.closed:
            try:
                msg = conn.reader.read_frame()
            except (socket.timeout, TimeoutError):
                
                if room.is_active_player(conn):
                    log.warning("%s timed out on their turn", conn)
                    room.dispatch(EV_CLIENT_DISCONNECTED, conn)
                    return
                continue
            except MalformedFrameError as exc:
                log.warning("Malformed JSON from %s: %s", conn, exc)
                room.send_error(conn, E_MALFORMED_JSON,
                                "Frame is not valid JSON.")
                continue        
            except FrameTooLongError:
                log.warning("Oversized frame from %s", conn)
                room.send_error(conn, E_FRAME_TOO_LONG,
                                "Frame exceeded 4096 bytes.")
                room.dispatch(EV_CLIENT_DISCONNECTED, conn)
                return

            if msg is None:     
                if not conn.closed:
                    log.info("%s closed the connection (EOF)", conn)
                    room.dispatch(EV_CLIENT_DISCONNECTED, conn)
                return

            #validate the envelope
            problem = validate_envelope(msg)
            if problem:
                room.send_error(conn, *problem)
                continue
            mtype = msg["msg_type"]
            log.info("<- %s %s %s", conn, mtype, msg["payload"] or "")

            #route to the FSM
            if mtype == MSG_CONNECT:
                room.dispatch(EV_CONNECT, conn,
                              {"player_id": msg["player_id"]})
            elif mtype == MSG_MOVE:
                problem = validate_move_payload(msg["payload"])
                if problem:
                    room.send_error(conn, *problem)
                    continue
                room.dispatch(EV_MOVE, conn, {"row": msg["payload"]["row"],
                                              "col": msg["payload"]["col"]})
            elif mtype == MSG_DISCONNECT:
                room.dispatch(EV_DISCONNECT, conn)
                return
            else:
                room.send_error(conn, E_MALFORMED_MESSAGE,
                                f"{mtype} may only be sent by the server.")

    except (ConnectionResetError, BrokenPipeError,
            ConnectionAbortedError) as exc:
        if not conn.closed:
            log.warning("Connection to %s lost abruptly: %s", conn, exc)
            room.dispatch(EV_CLIENT_DISCONNECTED, conn)
    except OSError as exc:
        if not conn.closed:
            log.warning("Socket error on %s: %s", conn, exc)
            room.dispatch(EV_CLIENT_DISCONNECTED, conn)
    except Exception:   # last-resort safety net: never crash the server
        log.exception("Unexpected error serving %s", conn)
        if not conn.closed:
            room.dispatch(EV_CLIENT_DISCONNECTED, conn)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="TTT-1 Tic-Tac-Toe game server (CS 457). "
                    "Listens on all interfaces (0.0.0.0) for two players.",
        epilog="Example: python3 server.py -p 8080",
    )
    parser.add_argument("-p", "--port", type=int, required=True,
                        help="TCP port to listen on (1-65535)")
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")
    return args


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%H:%M:%S")

    room = GameRoom()
    log.info("FSM state: %s", room.fsm.state)

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        listener.bind(("0.0.0.0", args.port))
    except OSError as exc:
        log.error("Cannot bind 0.0.0.0:%d -- %s", args.port, exc)
        sys.exit(1)
    listener.listen()
    log.info("Listening on 0.0.0.0:%d", args.port)
    with room.lock:
        room.fsm.handle(EV_SOCKET_BOUND)

    try:
        while True:
            sock, addr = listener.accept()
            conn = Connection(sock, addr)
            log.info("Accepted TCP connection from %s", conn)
            threading.Thread(target=serve_client, args=(conn, room),
                             name=f"client-{conn}", daemon=True).start()
    except KeyboardInterrupt:
        log.info("Ctrl+C received, shutting down server.")
    finally:
        listener.close()


if __name__ == "__main__":
    main()