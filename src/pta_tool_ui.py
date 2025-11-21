# pta_tool_ui.py
import json
import threading
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from pta_tool_class import PTAContestGenerator

CONFIG_FILE = "pta_config.json"


class ConfigWindow(tk.Toplevel):
    """Cookie配置窗口"""

    def __init__(self, parent):
        super().__init__(parent)
        self.title("配置Cookie")
        self.geometry("400x250")
        self.parent = parent
        self.cookies = {
            "_bl_uid": tk.StringVar(),
            "_ga": tk.StringVar(),
            "_ga_ZHCNP8KECW": tk.StringVar(),
            "JSESSIONID": tk.StringVar(),
            "PTASession": tk.StringVar()
        }

        self._create_widgets()

    def _create_widgets(self):
        """创建配置表单"""
        ttk.Label(self, text="Cookie配置").pack(pady=5)

        # 输入字段
        fields = [
            ("_bl_uid", "请输入_bl_uid:"),
            ("_ga", "请输入_ga:"),
            ("_ga_ZHCNP8KECW", "请输入_ga_ZHCNP8KECW:"),
            ("JSESSIONID", "请输入JSESSIONID:"),
            ("PTASession", "请输入PTASession:")
        ]

        for key, label in fields:
            frame = ttk.Frame(self)
            frame.pack(fill=tk.X, padx=10, pady=2)
            ttk.Label(frame, text=label, width=15).pack(side=tk.LEFT)
            ttk.Entry(frame, textvariable=self.cookies[key], width=30).pack(side=tk.RIGHT)

        # 操作按钮
        btn_frame = ttk.Frame(self)
        btn_frame.pack(pady=10)
        ttk.Button(btn_frame, text="加载配置", command=self._load_config).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="保存配置", command=self._save_config).pack(side=tk.RIGHT, padx=5)

    def _load_config(self):
        """加载配置文件"""
        file_path = filedialog.askopenfilename(filetypes=[("JSON文件", "*.json")])
        if file_path:
            try:
                with open(file_path, "r") as f:
                    config = json.load(f)
                    for key in self.cookies:
                        self.cookies[key].set(config.get(key, ""))
                self.parent.generator.set_cookies(config)
                messagebox.showinfo("成功", "配置加载成功")
                self.destroy()
            except Exception as e:
                messagebox.showerror("错误", f"加载失败: {str(e)}")

    def _save_config(self):
        """保存配置文件并更新生成器的Cookie"""
        config = {key: var.get() for key, var in self.cookies.items()}
        try:
            with open(CONFIG_FILE, "w") as f:
                json.dump(config, f, indent=2)
            # 使用业务层提供的方法更新Cookie
            self.parent.generator.set_cookies(config)
            messagebox.showinfo("成功", f"配置已保存至{CONFIG_FILE}")
            self.destroy()
        except Exception as e:
            messagebox.showerror("错误", f"保存失败: {str(e)}")


class MainApplication(tk.Tk):
    """主应用程序"""

    def __init__(self):
        super().__init__()
        self.title("PTA比赛生成器")
        self.geometry("800x600")

        self.generator = PTAContestGenerator()
        self.problem_sets = []
        self.selected_id = None

        self._check_config()
        self._create_widgets()

    def _check_config(self):
        """检查配置文件"""
        try:
            with open(CONFIG_FILE, "r") as f:
                config = json.load(f)
                self.generator.set_cookies(config)
        except FileNotFoundError:
            messagebox.showwarning("提示", "请先配置Cookie信息")
            self.open_config_window()

    def _create_widgets(self):
        """创建界面组件"""
        # 工具栏
        toolbar = ttk.Frame(self)
        toolbar.pack(fill=tk.X, padx=2, pady=2)

        ttk.Button(toolbar, text="配置Cookie", command=self.open_config_window).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="刷新列表", command=self.load_problem_sets).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="生成XML", command=self.generate_xml).pack(side=tk.RIGHT, padx=2)

        # 题目集列表
        self.tree = ttk.Treeview(self, columns=("id", "name", "time"), show="headings")
        self.tree.heading("id", text="ID")
        self.tree.heading("name", text="名称")
        # self.tree.heading("count", text="题目数")
        self.tree.heading("time", text="开始时间")
        self.tree.column("id", width=100)
        self.tree.column("name", width=300)
        # self.tree.column("count", width=80)
        self.tree.column("time", width=200)
        self.tree.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # 状态栏
        self.status = ttk.Label(self, text="就绪", relief=tk.SUNKEN)
        self.status.pack(side=tk.BOTTOM, fill=tk.X)

    def open_config_window(self):
        """打开配置窗口"""
        ConfigWindow(self)

    def load_problem_sets(self):
        """加载题目集列表"""

        def _load():
            try:
                self.status.config(text="正在加载题目集...")
                self.problem_sets = self.generator.get_problem_sets()
                self.tree.delete(*self.tree.get_children())
                for ps in self.problem_sets:
                    self.tree.insert("", "end", values=(
                        ps["id"],
                        ps["name"],
                        # ps["problem_count"],
                        ps["start_time"] or "未设置"
                    ))
                self.status.config(text=f"加载完成，共{len(self.problem_sets)}个题目集")
            except Exception as e:
                messagebox.showerror("错误", str(e))
                self.status.config(text="加载失败")

        threading.Thread(target=_load, daemon=True).start()

    def generate_xml(self):
        """生成XML文件"""
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("提示", "请先选择题目集")
            return

        item = self.tree.item(selected[0])
        problem_id = item["values"][0]

        def _generate():
            try:
                self.status.config(text="正在生成XML...")
                self.generator.select_problem_set(problem_id)
                output_path = filedialog.asksaveasfilename(
                    defaultextension=".xml",
                    filetypes=[("XML文件", "*.xml")]
                )
                if output_path:
                    self.generator.generate_contest_xml(output_path)
                    messagebox.showinfo("成功", f"文件已生成至:\n{output_path}")
                    self.status.config(text="生成完成")
            except Exception as e:
                messagebox.showerror("错误", str(e))
                print("1")
                self.status.config(text="生成失败")

        threading.Thread(target=_generate, daemon=True).start()


if __name__ == "__main__":
    app = MainApplication()
    app.mainloop()
