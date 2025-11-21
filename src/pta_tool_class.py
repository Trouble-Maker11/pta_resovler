import logging
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PTAAuthError(Exception):
    """自定义异常：认证失败（Cookie过期）"""
    pass


class PTAAPIError(Exception):
    """自定义异常：API响应错误"""
    pass


class PTAContestGenerator:
    def __init__(self, organization="HBUE", region_id="1"):
        """
        初始化生成器
        :param organization: 学校/组织名称，默认 HBUE
        :param region_id: 外部ID，默认 1
        """
        self.organization = organization
        self.region_id = region_id
        self.session = self._init_session()

        # 状态变量
        self.selected_problem_set_id = None
        self.contest_root = None
        self.label_map = {}
        self.exam_info = {}

        # 编译器映射表 (PTA Compiler String -> ICPC Language ID)
        # 根据实际抓包情况，PTA通常返回 GXX(C++), GCC(C), JAVA(Java), PYTHON3(Python)

        self.compiler_map = {
            "GCC": "1", "CLANG": "1",
            "GXX": "2", "CLANGXX": "2", "C++": "2",
            "JAVA": "3",
            "PYTHON3": "4", "PYPY3": "4"
        }

        # 添加PTA状态到icpctools acronym的映射
        self.result_map = {
            "ACCEPTED": "AC",
            "WRONG_ANSWER": "WA",
            "TIME_LIMIT_EXCEEDED": "TLE",
            "COMPILE_ERROR": "CE",
            "SEGMENTATION_FAULT": "SF",
            "FLOAT_POINT_EXCEPTION": "FPE",
            "MEMORY_LIMIT_EXCEEDED": "MLE",
            "NON_ZERO_EXIT_CODE": "NZEC",
            "RUNTIME_ERROR": "RE",
            "PRESENTATION_ERROR": "PE",
            "OUTPUT_LIMIT_EXCEEDED": "OLE",
        }
    def _init_session(self):
        """初始化带有重试机制的会话"""
        session = requests.Session()

        # 配置重试策略：重试3次，针对 500, 502, 503, 504 错误
        retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        session.mount('https://', adapter)
        session.mount('http://', adapter)

        # 伪装头
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://pintia.cn/",
            "Accept": "application/json, text/plain, */*"
        })
        return session

    def set_cookies(self, cookies):
        """设置Cookies"""
        if not cookies:
            logger.warning("尝试设置空的Cookies")
            return
        self.session.cookies.update(cookies)

    def _safe_request(self, url, params=None):
        """
        安全的网络请求封装
        功能增强：出错时自动保存 HTML 到本地以便调试，并打印状态码
        """
        try:
            resp = self.session.get(url, params=params, timeout=15)

            # --- 调试信息输出 ---
            content_type = resp.headers.get('Content-Type', '')
            logger.info(f"请求 URL: {resp.url}")
            logger.info(f"状态码: {resp.status_code} | 类型: {content_type}")

            # 检查是否出错 (状态码非200 或 不是JSON)
            is_error = resp.status_code != 200
            is_not_json = 'application/json' not in content_type

            if is_error or is_not_json:
                # --- 核心修改：保存 HTML 文件 ---
                import os
                filename = "pta_error_dump.html"
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(resp.text)

                abs_path = os.path.abspath(filename)
                print("\n" + "=" * 50)
                print(f"!!! 异常检测 !!!")
                print(f"状态码: {resp.status_code}")
                print(f"响应类型: {content_type}")
                print(f"响应内容已保存至: {abs_path}")
                print("请用浏览器打开该文件，查看是否为登录页面或报错页面。")
                print("=" * 50 + "\n")

                # 抛出具体的异常信息
                if resp.status_code == 401:
                    raise PTAAuthError(f"认证失败 [401]。请检查 Cookie。")
                elif is_not_json:
                    # 读取前100个字符用于报错提示
                    snippet = resp.text[:100].replace('\n', ' ')
                    raise PTAAPIError(f"期望 JSON 但收到 HTML (状态码 {resp.status_code})。内容片段: {snippet}...")
                else:
                    raise PTAAPIError(f"API 请求失败，状态码: {resp.status_code}")

            return resp.json()

        except requests.RequestException as e:
            raise PTAAPIError(f"网络连接底层错误: {str(e)}")

    # ----------------- 业务逻辑 -----------------

    def get_problem_sets(self):
        """获取所有可用题目集"""
        all_sets = []
        page = 0
        limit = 50

        logger.info("正在获取题目集列表...")
        try:
            while True:
                url = "https://pintia.cn/api/problem-sets/admin"
                params = {
                    "sort_by": '{"type":"UPDATE_AT","asc":false}',
                    "page": page,
                    "limit": limit,
                    "filter": '{"ownerId":"0"}'
                }

                data = self._safe_request(url, params=params)
                current = data.get("problemSets", [])
                if not current:
                    break

                all_sets.extend(current)
                if len(current) < limit:
                    break
                page += 1

            return [{
                "name": ps.get("name", "未命名题目集"),
                "id": ps.get("id"),
                "start_time": ps.get("startAt"),
            } for ps in all_sets]

        except Exception as e:
            logger.error(f"获取题目集失败: {e}")
            raise

    def select_problem_set(self, problem_set_id):
        """选中并验证题目集"""
        self.selected_problem_set_id = problem_set_id
        logger.info(f"验证题目集 ID: {problem_set_id}")
        # 验证有效性
        url = f"https://pintia.cn/api/problem-sets/{problem_set_id}/exams"
        self._safe_request(url)  # 如果失败会抛出异常

    def generate_contest_xml(self, output_path="contest.xml"):
        """生成流程主控"""
        if not self.selected_problem_set_id:
            raise ValueError("请先选择题目集")

        logger.info("开始生成 XML...")
        self._init_xml_structure()

        # 按顺序执行各步骤
        self._process_exam_info()
        self._add_static_nodes()
        self._process_problems()
        self._process_teams()
        self._process_submissions()
        self._add_finalized_node()

        self._save_xml(output_path)
        logger.info(f"XML 生成完毕: {output_path}")
        return output_path

    # ----------------- XML 构建细节 -----------------

    def _init_xml_structure(self):
        self.contest_root = ET.Element("contest")

    def _process_exam_info(self):
        """处理比赛基本信息"""
        url = f'https://pintia.cn/api/problem-sets/{self.selected_problem_set_id}'
        # 使用 safe_request 获取数据
        self.exam_info = self._safe_request(url)

        info = ET.SubElement(self.contest_root, "info")
        problem_set = self.exam_info.get("problemSet", {})

        # 1. 时间处理：解析 startAt 和 endAt
        start_at_str = problem_set.get("startAt", "1970-01-01T00:00:00Z")
        end_at_str = problem_set.get("endAt", "1970-01-01T00:00:00Z")

        # 辅助函数：处理 ISO 时间字符串 (兼容 Python 3.6+ 的 Z 结尾)
        def parse_time(time_str):
            if time_str.endswith('Z'):
                return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            return datetime.fromisoformat(time_str)

        start_dt = parse_time(start_at_str)
        end_dt = parse_time(end_at_str)

        # 将开始时间存为成员变量（统一转为 UTC 以便后续计算相对时间）
        self.contest_start_dt = start_dt.astimezone(timezone.utc)

        # 2. 时长处理 (双重保险逻辑)
        # 优先直接读取 duration 字段
        duration_sec = problem_set.get("duration", 0)

        # 如果 duration 为 0 或不存在 尝试通过 结束时间 - 开始时间 计算
        if duration_sec <= 0:
            try:
                delta = end_dt - start_dt
                duration_sec = int(delta.total_seconds())
            except Exception:
                duration_sec = 18000  # 如果都失败了，兜底默认 5 小时

        # 格式化为 H:MM:SS
        m, s = divmod(duration_sec, 60)
        h, m = divmod(m, 60)
        duration_str = f"{h}:{m:02d}:{s:02d}"

        # 3. 写入 XML 节点
        ET.SubElement(info, "title").text = problem_set.get("name", "PTA Contest")
        ET.SubElement(info, "short-title").text = problem_set.get("name", "PTA Contest")
        ET.SubElement(info, "contest-id").text = str(problem_set.get("id", "0"))
        ET.SubElement(info, "starttime").text = f"{self.contest_start_dt.timestamp():.1f}"
        ET.SubElement(info, "length").text = duration_str
        ET.SubElement(info, "penalty").text = "20"
        ET.SubElement(info, "started").text = "False"
        ET.SubElement(info, "scoreboard-freeze-length").text = "1:00:00"

    def _add_static_nodes(self):
        """添加静态定义 (Judgement, Language, Region)"""
        # 1. Region
        region = ET.SubElement(self.contest_root, "region")
        ET.SubElement(region, "external-id").text = self.region_id
        ET.SubElement(region, "name").text = self.organization

        # 2. Judgements (判题结果定义)
        judgements_conf = [
            ("1", "AC", "ACCEPTED", "true", "false"),
            ("2", "SF", "SEGMENTATION_FAULT", "false", "true"),
            ("3", "WA", "WRONG_ANSWER", "false", "true"),
            ("4", "TLE", "TIME_LIMIT_EXCEEDED", "false", "true"),
            ("5", "CE", "COMPILE_ERROR", "false", "false"),
            ("6", "FPE", "FLOAT_POINT_EXCEPTION", "false", "true"),
            ("7", "MLE", "MEMORY_LIMIT_EXCEEDED", "false", "true"),
            ("8", "NZEC", "NON_ZERO_EXIT_CODE", "false", "true"),
            ("9", "RE", "RUNTIME_ERROR", "false", "true"),
            ("10", "PE", "PRESENTATION_ERROR", "false", "true"),
            ("11", "OLE", "OUTPUT_LIMIT_EXCEEDED", "false", "true"),
        ]
        for j_id, acr, name, solved, penalty in judgements_conf:
            node = ET.SubElement(self.contest_root, "judgement")
            ET.SubElement(node, "id").text = j_id
            ET.SubElement(node, "acronym").text = acr
            ET.SubElement(node, "name").text = name
            ET.SubElement(node, "solved").text = solved
            ET.SubElement(node, "penalty").text = penalty

        # 3. Languages (语言定义)
        languages = [("1", "c"), ("2", "c++"), ("3", "java"), ("4", "python")]
        for l_id, l_name in languages:
            lang_node = ET.SubElement(self.contest_root, "language")
            ET.SubElement(lang_node, "id").text = l_id
            ET.SubElement(lang_node, "name").text = l_name

    def _process_problems(self):
        """处理题目"""
        url = f'https://pintia.cn/api/problem-sets/{self.selected_problem_set_id}/preview/problems'
        params = {"problem_type": "PROGRAMMING", "page": 0, "limit": 500}
        data = self._safe_request(url, params)

        problems = data.get("problemSetProblems", [])

        for idx, p in enumerate(problems):
            p_id = p.get("id")
            xml_id = str(idx + 1)
            letter = chr(65 + idx)  # A, B, C...

            # 存入映射表供 Submission 使用
            self.label_map[p_id] = {"xml_id": xml_id, "letter": letter}

            node = ET.SubElement(self.contest_root, "problem")
            ET.SubElement(node, "id").text = xml_id
            ET.SubElement(node, "letter").text = letter
            ET.SubElement(node, "name").text = f"Problem {letter}"  # 或者使用 p.get("title")

    def _process_teams(self):
        """处理团队信息 - 使用 user-group-members 接口获取完整映射数据"""
        logger.info("开始拉取队伍信息...")

        # 使用 user-group-members 接口，它返回完整的用户映射表
        base_url = f'https://pintia.cn/api/problem-sets/{self.selected_problem_set_id}/user-group-members'
        limit = 20
        page = 0
        total_processed = 0

        # 在实例中存储映射表，供 _add_team_node 使用
        self._exam_by_user_id = {}
        self._student_user_by_id = {}

        try:
            while True:
                params = {
                    "exam_status": "UNKNOWN",
                    "page": page,
                    "limit": limit,
                    "order_by": "startAt",
                    "asc": "false"
                }

                data = self._safe_request(base_url, params)

                # 第一次调用时获取总数
                if page == 0:
                    total = data.get("total", 0)
                    if total == 0:
                        logger.warning("未找到任何队伍")
                        return
                    logger.info(f"共有 {total} 支队伍需要抓取")

                # 更新映射表（后续页面可能包含更多用户数据）
                self._exam_by_user_id.update(data.get("examByUserId", {}))
                self._student_user_by_id.update(data.get("studentUserById", {}))

                # 处理当前页的成员列表
                members = data.get("userGroupMembers", [])
                if not members:
                    logger.warning(f"第 {page + 1} 页无数据")
                    break

                # 为每个成员生成team节点
                for member in members:
                    self._add_team_node(member)
                    total_processed += 1

                logger.info(f"已处理第 {page + 1} 页，共 {len(members)} 支队伍")

                # 分页判断：如果返回数量小于limit，说明已到最后一页
                if len(members) < limit:
                    break

                page += 1

            logger.info(f"队伍信息处理完成，共处理 {total_processed} 支队伍")

        except Exception as e:
            logger.error(f"获取队伍信息失败: {e}")
            raise

    def _add_team_node(self, member_data):
        """生成单个 Team 节点"""
        user_id = member_data.get("userId")
        student_user_id = member_data.get("studentUserId")

        if not user_id:
            logger.warning(f"成员数据缺少userId: {member_data}")
            return

        # 从映射表中获取队伍名称
        name = self._get_team_name(user_id, student_user_id)

        # 创建team节点
        team = ET.SubElement(self.contest_root, "team")
        ET.SubElement(team, "id").text = str(user_id)
        ET.SubElement(team, "external-id").text = self.region_id
        ET.SubElement(team, "region").text = self.organization
        ET.SubElement(team, "name").text = name
        ET.SubElement(team, "university").text = self.organization

    def _get_team_name(self, user_id, student_user_id):
        """根据userId和studentUserId从映射表中获取队伍名称"""
        name = ""

        # 方法1：优先从 examByUserId 获取（包含完整考试信息）
        if hasattr(self, '_exam_by_user_id') and user_id in self._exam_by_user_id:
            exam_info = self._exam_by_user_id[user_id]
            student_user = exam_info.get("studentUser", {})
            name = student_user.get("name", "").strip()
            if name:
                return name

        # 方法2：从 studentUserById 获取（备用）
        if hasattr(self, '_student_user_by_id') and student_user_id in self._student_user_by_id:
            student_user = self._student_user_by_id[student_user_id]
            name = student_user.get("name", "").strip()
            if name:
                return name

        # 方法3：如果都找不到，使用默认名称
        name = f"Team_{user_id}"
        logger.warning(f"无法找到用户 {user_id} 的名称信息，使用默认队伍名: {name}")
        return name


    def _process_submissions(self):
        """处理提交记录"""
        base_url = f"https://pintia.cn/api/problem-sets/{self.selected_problem_set_id}/submissions"
        before = None
        counter = 1

        logger.info("开始抓取提交记录...")

        while True:
            params = {"limit": 100}
            if before:
                params["before"] = before

            # 发送请求
            data = self._safe_request(base_url, params)
            submissions = data.get("submissions", [])

            # 如果列表为空，说明没有数据了，停止
            if not submissions:
                break

            processed_this_page = 0 # 计数器
            # 遍历处理当前页的提交
            for sub in submissions:
                self._add_submission_node(sub, counter)
                counter += 1
                processed_this_page += 1
                if processed_this_page % 100 == 0:
                    logger.info(f"已处理 {processed_this_page} 条提交，短暂休眠0.5秒...")
                    time.sleep(0.3)

            # --- 分页逻辑核心修改 ---

            # 1. 检查 hasBefore 字段 (JSON中 explicit 声明是否还有更早的数据)
            has_more = data.get("hasBefore", False)

            # 2. 获取游标：取本页最后一条提交记录的 ID
            if has_more:
                last_submission = submissions[-1]
                # 下一页的 cursor 是当前页最后一个元素的 ID
                next_cursor = last_submission.get("id")

                # 安全检查：防止游标死循环
                if next_cursor == before:
                    logger.warning("检测到游标未变化，为防止死循环强制停止抓取")
                    break

                before = next_cursor
                logger.info(f"准备抓取下一页，游标(before): {before}")
            else:
                logger.info("没有更多历史数据 (hasBefore=False)，抓取结束")
                break

    def _add_submission_node(self, sub, counter):
        """生成单条 Run 记录"""
        problem_id = sub.get("problemSetProblemId")
        if problem_id not in self.label_map:
            return  # 忽略未知题目的提交

        problem_conf = self.label_map[problem_id]

        # 状态映射 - 核心修复点
        pta_status = sub.get("status", "UNKNOWN")
        solved = "true" if pta_status == "ACCEPTED" else "false"
        penalty = "false" if pta_status == "COMPILE_ERROR" else "true"

        # 将PTA状态转换为icpctools标准的acronym
        result_acronym = self.result_map.get(pta_status, "WA")

        # 语言映射
        compiler = sub.get("compiler", "UNKNOWN").upper()
        lang_id = self.compiler_map.get(compiler, "1")

        # 时间计算
        submit_at = datetime.fromisoformat(sub["submitAt"].replace("Z", "+00:00"))
        time_diff = submit_at - self.contest_start_dt
        rel_time_sec = int(time_diff.total_seconds())
        if rel_time_sec < 0:
            rel_time_sec = 0

        # 构建run节点
        run = ET.SubElement(self.contest_root, "run")
        ET.SubElement(run, "id").text = str(counter)
        ET.SubElement(run, "judged").text = "True"
        ET.SubElement(run, "language").text = lang_id
        ET.SubElement(run, "problem").text = problem_conf["xml_id"]
        ET.SubElement(run, "status").text = "done"
        ET.SubElement(run, "team").text = sub.get("userId")
        ET.SubElement(run, "time").text = str(rel_time_sec)
        ET.SubElement(run, "timestamp").text = f"{submit_at.timestamp():.2f}"
        ET.SubElement(run, "solved").text = solved
        ET.SubElement(run, "penalty").text = penalty
        ET.SubElement(run, "result").text = result_acronym  # 关键修复：使用acronym短格式

    def _add_finalized_node(self):
        """添加结束标记"""
        fin = ET.SubElement(self.contest_root, "finalized")
        ET.SubElement(fin, "last_gold").text = "1"
        ET.SubElement(fin, "last_silver").text = "1"
        ET.SubElement(fin, "last_bronze").text = "1"
        ET.SubElement(fin, "time").text = str(self.contest_root.find("info/length").text)
        ET.SubElement(fin, "timestamp").text = str(datetime.now().timestamp())

    def _save_xml(self, path):
        """保存并美化XML"""
        self._indent(self.contest_root)
        tree = ET.ElementTree(self.contest_root)
        tree.write(path, encoding="utf-8", xml_declaration=True)

    @staticmethod
    def _indent(elem, level=0):
        """XML 缩进美化 (In-place)"""
        i = "\n" + level * "  "
        if len(elem):
            if not elem.text or not elem.text.strip():
                elem.text = i + "  "
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
            for child in elem:
                PTAContestGenerator._indent(child, level + 1)
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
        else:
            if level and (not elem.tail or not elem.tail.strip()):
                elem.tail = i
