"""Snake (贪吃蛇) - Python 小游戏

依赖：仅标准库（curses）。
运行：python3 snake.py
说明：
- 方向键控制（↑↓←→）
- P 暂停/继续
- R 重新开始
- Q 退出

注意：Windows 原生终端可能没有 curses；可用 WSL / Git Bash / Windows Terminal + WSL 运行。
"""

import curses
import random
import time


KEYS = {
    "UP": {curses.KEY_UP, ord("w"), ord("W")},
    "DOWN": {curses.KEY_DOWN, ord("s"), ord("S")},
    "LEFT": {curses.KEY_LEFT, ord("a"), ord("A")},
    "RIGHT": {curses.KEY_RIGHT, ord("d"), ord("D")},
}

DIRS = {
    "UP": (-1, 0),
    "DOWN": (1, 0),
    "LEFT": (0, -1),
    "RIGHT": (0, 1),
}

OPPOSITE = {
    "UP": "DOWN",
    "DOWN": "UP",
    "LEFT": "RIGHT",
    "RIGHT": "LEFT",
}


def place_food(h, w, snake):
    # 可用区域：去掉边框
    while True:
        y = random.randint(1, h - 2)
        x = random.randint(1, w - 2)
        if (y, x) not in snake:
            return (y, x)


def draw_border(stdscr, h, w):
    stdscr.addstr(0, 0, "#" * w)
    for y in range(1, h - 1):
        stdscr.addstr(y, 0, "#")
        stdscr.addstr(y, w - 1, "#")
    stdscr.addstr(h - 1, 0, "#" * w)


def center_text(stdscr, y, text):
    h, w = stdscr.getmaxyx()
    x = max(0, (w - len(text)) // 2)
    try:
        stdscr.addstr(y, x, text)
    except curses.error:
        pass


def game(stdscr):
    curses.curs_set(0)
    stdscr.nodelay(True)
    stdscr.keypad(True)

    speed = 10  # ticks per second
    tick = 1.0 / speed

    while True:
        stdscr.clear()
        h, w = stdscr.getmaxyx()

        # 画面太小就提示
        if h < 10 or w < 30:
            center_text(stdscr, h // 2, "窗口太小，请放大终端（至少 10x30）")
            stdscr.refresh()
            time.sleep(0.2)
            continue

        # 初始化蛇
        start_y = h // 2
        start_x = w // 2
        snake = [(start_y, start_x - 1), (start_y, start_x), (start_y, start_x + 1)]
        direction = "RIGHT"
        pending_dir = direction

        score = 0
        paused = False
        game_over = False

        food = place_food(h, w, snake)

        last = time.time()
        while True:
            now = time.time()
            dt = now - last
            if dt < tick:
                time.sleep(max(0.0, tick - dt))
            last = time.time()

            # 输入
            key = stdscr.getch()
            if key != -1:
                if key in (ord("q"), ord("Q")):
                    return
                if key in (ord("p"), ord("P")) and not game_over:
                    paused = not paused
                if key in (ord("r"), ord("R")):
                    break

                if not paused and not game_over:
                    for name, codes in KEYS.items():
                        if key in codes:
                            # 不能直接反向
                            if name != OPPOSITE[direction]:
                                pending_dir = name
                            break

            # 更新状态
            if not paused and not game_over:
                direction = pending_dir
                dy, dx = DIRS[direction]
                head_y, head_x = snake[-1]
                new_head = (head_y + dy, head_x + dx)

                # 撞墙
                if new_head[0] in (0, h - 1) or new_head[1] in (0, w - 1):
                    game_over = True
                else:
                    # 撞自己（允许尾巴移动后空出来，所以先计算是否吃到）
                    will_eat = (new_head == food)
                    body = snake if will_eat else snake[1:]
                    if new_head in body:
                        game_over = True
                    else:
                        snake.append(new_head)
                        if will_eat:
                            score += 1
                            # 加速一点（有上限）
                            speed = min(25, 10 + score)
                            tick = 1.0 / speed
                            food = place_food(h, w, snake)
                        else:
                            snake.pop(0)

            # 绘制
            stdscr.erase()
            draw_border(stdscr, h, w)

            # HUD
            hud = f"Score: {score}  Speed: {speed}  (P暂停 R重开 Q退出)"
            try:
                stdscr.addstr(0, 2, hud[: w - 4])
            except curses.error:
                pass

            # 食物
            fy, fx = food
            try:
                stdscr.addstr(fy, fx, "*")
            except curses.error:
                pass

            # 蛇
            for (y, x) in snake[:-1]:
                try:
                    stdscr.addstr(y, x, "o")
                except curses.error:
                    pass
            hy, hx = snake[-1]
            try:
                stdscr.addstr(hy, hx, "@")
            except curses.error:
                pass

            if paused:
                center_text(stdscr, h // 2, "Paused")
            if game_over:
                center_text(stdscr, h // 2 - 1, "GAME OVER")
                center_text(stdscr, h // 2, "按 R 重新开始，或 Q 退出")

            stdscr.refresh()


def main():
    curses.wrapper(game)


if __name__ == "__main__":
    main()
