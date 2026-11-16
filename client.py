#!/usr/bin/env python3
"""
client.py -- TTT-1 Tic-Tac-Toe game client (CS 457, Sprint 3).

Usage:
    python3 client.py -h
    python3 client.py -i server.tiemtore.edu -p 8080
    python3 client.py -i server.tiemtore.edu -p 8080 -n Kadre
"""

import argparse
import json
import socket
import sys
import threading
import time




MAX_FRAME_BYTES = 4096          # includes the trailing newline
DELIMITER = b"\n"               # 0x0A frame terminator
RECV_CHUNK = 4096               # bytes requested per recv() call

# The 8 message types in the inventory (section 5).
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


# ---------------------------------------------------------------------------
# Exceptions raised by the frame reader
# ---------------------------------------------------------------------------

class ProtocolError(Exception):
    """Base class for TTT-1 protocol violations."""


class FrameTooLongError(ProtocolError):
    """A frame exceeded MAX_FRAME_BYTES (section 3.1); connection closes."""


class MalformedFrameError(ProtocolError):
    """A complete frame was not valid UTF-8 JSON (section 9).

    The connection stays open (single-strike policy).
    """


# ---------------------------------------------------------------------------
# Encoding (section 3.1 / ai_prompts.md task 3.1)
# ---------------------------------------------------------------------------

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
    """Build an envelope dict (not yet encoded). Used by the server FSM."""
    return {
        "msg_type": msg_type,
        "player_id": player_id,
        "payload": payload,
        "timestamp": int(time.time()),
    }


def make_error(player_id, code, detail):
    """Build an ERROR envelope (section 6.6), clipping detail to 200 chars."""
    payload = {"code": code, "detail": detail[:MAX_DETAIL_LEN]}
    return make_message(MSG_ERROR, player_id, payload)


def send_message(sock, message):
    """Encode an envelope dict and send it with sendall() (section 8.3)."""
    sock.sendall(encode_message(message["msg_type"], message["player_id"],
                                message["payload"], message["timestamp"]))


# ---------------------------------------------------------------------------
# Buffered frame decoder (section 3.2 / ai_prompts.md task 3.2)
# ---------------------------------------------------------------------------

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
        """Decode one frame (without its newline) into a Python object."""
        try:
            return json.loads(frame.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MalformedFrameError(str(exc)) from exc


# Schema validation 

def _is_int(value):
    
    return isinstance(value, int) and not isinstance(value, bool)


def is_valid_alias(alias):
    
    return (isinstance(alias, str)
            and 1 <= len(alias) <= MAX_ALIAS_LEN
            and all(0x20 <= ord(ch) <= 0x7E for ch in alias))


def validate_envelope(msg):
    
    if not isinstance(msg, dict):
        return E_MALFORMED_MESSAGE, "Frame must be a JSON object."
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


#Board helpers

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


# END OF SHARED PROTOCOL SECTION

CONNECT_TIMEOUT_SEC = 10


class ClientState:

    def __init__(self, alias):
        self.my_id = alias
        self.role = None
        self.opponent = None
        self.board = new_board()
        self.fsm = "Lobby"
        self.my_turn = threading.Event()
        self.ended = threading.Event()
        self.print_lock = threading.Lock()
        self.awaiting_input = False


def say(state, text=""):
    with state.print_lock:
        print(text, flush=True)


def show_scores(scores):
    """Format a scores dict like 'Kadre: 1 | Madison: 0'."""
    return " | ".join(f"{name}: {pts}" for name, pts in scores.items())



#Receiver thread

def handle_message(state, msg):
    mtype = msg.get("msg_type")
    payload = msg.get("payload", {})

    if mtype == MSG_LOBBY_WAIT:
        state.my_id = msg["player_id"]
        state.role = payload["role"]
        state.fsm = "Waiting"
        say(state, f"\nConnected as '{state.my_id}' "
                   f"(Player {payload['position']}, mark {state.role}).")
        say(state, "Waiting for Player 2 to join...")

    elif mtype == MSG_GAME_START:
        state.my_id = msg["player_id"]
        state.role = payload["role"]
        state.opponent = payload["opponent_id"]
        state.board = payload["board"]
        state.fsm = "Playing"
        say(state, "\n" + "=" * 40)
        say(state, f" GAME START: {state.my_id} ({state.role}) "
                   f"vs {state.opponent}")
        say(state, "=" * 40)
        show_turn(state, payload["first_turn"])

    elif mtype == MSG_STATE_UPDATE:
        state.board = payload["board"]
        say(state, f"\nScores -> {show_scores(payload['scores'])}")
        show_turn(state, payload["active_turn"])

    elif mtype == MSG_ERROR:
        code = payload.get("code")
        say(state, f"\n[SERVER ERROR] {code}: {payload.get('detail', '')}")
        if code in (E_ROOM_FULL, E_FRAME_TOO_LONG):
            end_game(state)
        elif (code in (E_INVALID_MOVE, E_MALFORMED_MESSAGE,
                       E_MALFORMED_JSON)
              and state.fsm == "MyTurn"):
            state.my_turn.set()        

    elif mtype == MSG_GAME_OVER:
        final = render_with_title(state, payload["final_board"],
                                  "FINAL BOARD")
        say(state, "\n" + final)
        outcome, winner = payload["outcome"], payload["winner"]
        if outcome == "DRAW":
            result = "It's a DRAW -- the board is full."
        elif outcome == "FORFEIT":
            if winner == state.my_id:
                result = "Your opponent left. You win by FORFEIT!"
            else:
                result = f"{winner} wins by FORFEIT (opponent left)."
        elif winner == state.my_id:
            result = "You WIN!"
        else:
            result = f"{winner} wins. Better luck next time."
        say(state, "=" * 40)
        say(state, f" GAME OVER: {result}")
        say(state, f" Final scores -> {show_scores(payload['final_scores'])}")
        say(state, "=" * 40)
        end_game(state)


def render_with_title(state, board, title):
    """Return the board rendering with a title line above it."""
    return f"{title}  (you are {state.role})\n{render_board(board)}"


def show_turn(state, active_turn):
    """Render the board and turn banner; wake the input thread if needed."""
    say(state, render_with_title(state, state.board, "BOARD"))
    if active_turn == state.my_id:
        state.fsm = "MyTurn"
        say(state, ">>> Your Turn <<<")
        state.my_turn.set()
    else:
        state.fsm = "Playing"
        state.my_turn.clear()
        say(state, f"Waiting for {active_turn}...")


def end_game(state):
    state.fsm = "Ended"
    state.ended.set()
    state.my_turn.set()


def receiver(sock, state):
    reader = FrameReader(sock)
    try:
        while not state.ended.is_set():
            try:
                msg = reader.read_frame()
            except MalformedFrameError:
                say(state, "[WARN] Ignored a malformed frame from the server.")
                continue
            if msg is None:                     # server closed the connection
                if not state.ended.is_set():
                    say(state, "\n[INFO] The server closed the connection.")
                break
            try:
                handle_message(state, msg)
            except (KeyError, TypeError, AttributeError):
                say(state, f"[WARN] Ignored an unexpected message: {msg!r}")
    except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError,
            FrameTooLongError) as exc:
        if not state.ended.is_set():
            say(state, f"\n[ERROR] Lost connection to the server: {exc}")
    except OSError:
        pass       
    finally:
        end_game(state)
        if state.awaiting_input:
            say(state, "Press Enter to exit.")

#Main thread: keyboard -> server

def read_move(state):
    while True:
        state.awaiting_input = True
        try:
            raw = input("Enter your move as 'row col' (0-2), or q to quit: ")
        except EOFError:
            return None
        finally:
            state.awaiting_input = False
        if state.ended.is_set():
            return None
        text = raw.strip().lower()
        if text in ("q", "quit", "exit"):
            return None
        parts = text.replace(",", " ").split()
        if len(parts) == 2 and all(x in ("0", "1", "2") for x in parts):
            return int(parts[0]), int(parts[1])
        say(state, "  Invalid format. Example: '1 2' means row 1, column 2.")


def send(sock, state, msg_type, payload):
    try:
        sock.sendall(encode_message(msg_type, state.my_id, payload))
        return True
    except OSError as exc:
        say(state, f"[ERROR] Could not send to server: {exc}")
        return False


def play(sock, state):
    while not state.ended.is_set():
        state.my_turn.wait()
        if state.ended.is_set():
            break
        state.my_turn.clear()
        move = read_move(state)
        if state.ended.is_set():
            break
        if move is None:
            say(state, "You quit the game (forfeit).")
            send(sock, state, MSG_DISCONNECT, {"reason": "quit"})
            return
        row, col = move
        if not send(sock, state, MSG_MOVE, {"row": row, "col": col}):
            return


#CLI parsing, DNS resolution, startup

def parse_args(argv=None):
    
    parser = argparse.ArgumentParser(
        description="TTT-1 Tic-Tac-Toe client (CS 457). Connects to the game "
                    "server by DNS hostname.",
        epilog="Example: python3 client.py -i server.tiemtore.edu -p 8080",
    )
    parser.add_argument("-i", "--host", dest="hostname", required=True,
                        help="server DNS hostname (e.g. server.tiemtore.edu)")
    parser.add_argument("-p", "--port", type=int, required=True,
                        help="server TCP port (1-65535)")
    parser.add_argument("-n", "--name", default=None,
                        help="player alias, 1-16 printable characters "
                             "(prompted if omitted)")
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")
    if args.name is not None and not is_valid_alias(args.name):
        parser.error("name must be 1-16 printable ASCII characters")
    return args


def resolve_hostname(hostname):

    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        print(f"[ERROR] Unable to resolve DNS hostname '{hostname}'.")
        print("Please check your Cisco router DNS configuration "
              "('ip dns server' / 'ip host') or verify domain spelling.")
        sys.exit(1)
    except UnicodeError:
        # Raised for malformed hostnames, e.g. an empty label ('a..edu').
        print(f"[ERROR] '{hostname}' is not a valid hostname. "
              "Please verify domain spelling.")
        sys.exit(1)


def ask_alias():
    while True:
        try:
            alias = input("Enter your player name (1-16 characters): ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(0)
        if is_valid_alias(alias):
            return alias
        print("  Name must be 1-16 printable ASCII characters.")


def connect(server_ip, hostname, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(CONNECT_TIMEOUT_SEC)
    try:
        sock.connect((server_ip, port))
    except ConnectionRefusedError:
        print(f"[ERROR] Connection refused by {hostname} "
              f"({server_ip}:{port}). "
              "Is server.py running on that port?")
        sys.exit(1)
    except (socket.timeout, TimeoutError):
        print(f"[ERROR] Timed out connecting to {hostname} "
              f"({server_ip}:{port}). "
              "Check routing between the subnets.")
        sys.exit(1)
    except OSError as exc:
        print(f"[ERROR] Could not connect to {hostname} "
              f"({server_ip}:{port}): {exc}")
        sys.exit(1)
    # Back to blocking mode for gameplay: a turn can take a while.
    sock.settimeout(None)
    return sock


def main():
    args = parse_args()
    server_ip = resolve_hostname(args.hostname)
    print(f"Resolved {args.hostname} -> {server_ip}")

    alias = args.name or ask_alias()
    sock = connect(server_ip, args.hostname, args.port)
    print(f"Connected to {args.hostname}:{args.port}")

    state = ClientState(alias)
    rx = threading.Thread(target=receiver, args=(sock, state), daemon=True)
    rx.start()

    try:
        if not send(sock, state, MSG_CONNECT, {}):
            sys.exit(1)
        play(sock, state)
    except KeyboardInterrupt:
        say(state, "\nCtrl+C -- leaving the game (forfeit).")
        if not state.ended.is_set():
            send(sock, state, MSG_DISCONNECT, {"reason": "forfeit"})
    finally:
        state.ended.set()
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()
        rx.join(timeout=1)


if __name__ == "__main__":
    main()