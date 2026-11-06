import tkinter as tk
from datetime import datetime, timedelta

import websocket


class AppA(tk.Tk):
    HOST = "127.0.0.1"
    PORT = 10050

    def __init__(self):
        super().__init__()
        self.title("Приложение A")
        self.geometry("300x220")
        self.resizable(False, False)

        self.date = datetime.now().replace(second=0, microsecond=0)

        self.time_lbl = tk.Label(self, font=("Arial", 20))
        self.time_lbl.pack(pady=20)

        btn_frame = tk.Frame(self)
        btn_frame.pack()
        tk.Button(btn_frame, text="− день", width=10, command=self._dec_day).pack(side="left", padx=5)
        tk.Button(btn_frame, text="+ день", width=10, command=self._inc_day).pack(side="left", padx=5)

        tk.Button(self, text="Отправить в B", command=self._send_to_b).pack(pady=15)

        self.status = tk.Label(self, fg="gray")
        self.status.pack()

        self._render()

    def _render(self):
        self.time_lbl.config(text=self.date.strftime("%Y-%m-%d %H:%M"))

    def _inc_day(self):
        self.date += timedelta(days=1)
        self._render()

    def _dec_day(self):
        self.date -= timedelta(days=1)
        self._render()

    def _send_to_b(self):
        text = self.date.strftime("%Y-%m-%d %H:%M")
        try:
            ws = websocket.create_connection(f"ws://{self.HOST}:{self.PORT}", timeout=3)
            ws.send(text)
            response = ws.recv()
            ws.close()
            self.status.config(text=f"Ответ от B: {response}", fg="green")
        except Exception as e:
            self.status.config(text=f"Ошибка подключения к B: {e}", fg="red")
            print(e)


if __name__ == "__main__":
    app = AppA()
    app.mainloop()
