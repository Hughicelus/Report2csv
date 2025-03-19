import uuid
import json
import logging
import os
import re
import time
import pandas as pd
from io import BytesIO
import msoffcrypto
import xlrd
from pathlib import Path
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import QApplication, QWidget, QFileDialog, QTableWidgetItem
from PySide6.QtSql import QSqlDatabase, QSqlQuery
from PySide6.QtCore import (
    QObject,
    QDateTime,
    Signal,
    Slot,
    QRunnable,
    QThreadPool,
)
from PySide6.QtUiTools import QUiLoader

logging.basicConfig(
    filename="./log/report2csv.log",
    filemode="a",
    encoding="utf-8",
    format="%(asctime)s %(message)s",
    level=logging.DEBUG,
)


class Signals(QObject):
    started = Signal(int)
    completed = Signal(tuple)
    save_data = Signal(pd.DataFrame, str, str)
    error = Signal(str)
    save_data = Signal(pd.DataFrame, str, str)


class Worker(QRunnable):
    def __init__(self, n, file):
        super().__init__()
        self.file = file
        self.n = n
        self.signals = Signals()
        self.setAutoDelete(True)  # 确保任务完成后自动清理

    @Slot()
    def run(self):
        try:
            self.signals.started.emit(self.n)
            if "-88" in self.file:
                result = self.process_88_card()
            elif "F32" in self.file:
                result = self.process_32_card()
            else:
                raise ValueError("Unsupported file type")

            combined_df, number, title, icmd, icmc, category = result
            msg = (self.n, self.file, number, title, icmd, icmc, category)
            self.signals.completed.emit(msg)
            self.signals.save_data.emit(combined_df, title, category)
        except Exception as e:
            logging.error(f"Error processing {self.file}: {str(e)}")

    def process_88_card(self):
        try:
            with pd.ExcelFile(self.file) as xls:
                category = "88卡"
                df = pd.read_excel(xls, sheet_name="88-SYNTH", header=None)
                number = df.iloc[4:6, 5].dropna().values[0]
                title = pd.read_excel(xls, sheet_name="88PRES", header=None).iloc[8, 6]
                icmd = float(df.iloc[27, 3])  # 确保数值类型
                icmc = float(df.iloc[27, 5])
                dfs = [
                    xls.parse(sheet).iloc[8:54, [15, 16, 17, 21, 22, 23, 24, 25, 26]]
                    for sheet in xls.sheet_names
                    if sheet.startswith("RES-")
                ]
                combined_df = pd.concat(dfs, ignore_index=True)
                combined_df.columns = [
                    "类型",
                    "编号",
                    "序号",
                    "上公差",
                    "下公差",
                    "零件1",
                    "零件2",
                    "零件3",
                    "零件4",
                ]
                combined_df.dropna(subset=["编号"], inplace=True)

            output_dir = Path("output")
            output_dir.mkdir(exist_ok=True)
            combined_df.to_csv(
                output_dir / f"{title}.csv", index=False, encoding="utf-8-sig"
            )
            return combined_df, number, title, icmd, icmc, category
        except Exception as e:
            logging.error(f"Error processing 88 card: {str(e)}")
            raise

    def process_32_card(self):
        category = "32卡"
        try:
            with pd.ExcelFile(self.file) as xls:
                df = pd.read_excel(xls, sheet_name="1(32j)", header=None)
                dfs = [
                    xls.parse(
                        sheet,
                        header=None,
                        usecols="X,AC,AE,AG,AH,AK,AL,AM,AN",
                        skiprows=9,
                        nrows=64,
                    )
                    for sheet in xls.sheet_names
                    if sheet.endswith("(32i)")
                ]
        except xlrd.biffh.XLRDError:
            decrypted = BytesIO()
            with open(self.file, "rb") as f:
                _file = msoffcrypto.OfficeFile(f)
                if _file.is_encrypted():
                    _file.load_key(password="VelvetSweatshop")
                    _file.decrypt(decrypted)
                    decrypted.seek(0)
                    df = pd.read_excel(decrypted, header=None, sheet_name="1(32j)")
                    decrypted.seek(0)
                    with pd.ExcelFile(decrypted) as xls:
                        dfs = [
                            xls.parse(
                                sheet,
                                header=None,
                                usecols="X,AC,AE,AG,AH,AK,AL,AM,AN",
                                skiprows=9,
                                nrows=64,
                            )
                            for sheet in xls.sheet_names
                            if sheet.endswith("(32i)")
                        ]
        number = df.iloc[2:5, 3].dropna().values[0].replace(" ", "")
        title = df.iloc[0, 2]
        icmd = float(df.iloc[22, 8])  # 转换为数值类型
        icmc = float(df.iloc[22, 6])
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df.columns = [
            "类型",
            "编号",
            "序号",
            "上公差",
            "下公差",
            "零件1",
            "零件2",
            "零件3",
            "零件4",
        ]
        combined_df.dropna(subset=["编号"], inplace=True)

        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        combined_df.to_csv(
            output_dir / f"{title}.csv", index=False, encoding="utf-8-sig"
        )
        return combined_df, number, title, icmd, icmc, category


class DatabaseWorker(QRunnable):
    def __init__(self, df, title, category, db_manager):
        super().__init__()
        self.df = df
        self.title = title
        self.category = category
        self.db_manager = db_manager
        self.setAutoDelete(True)
        self.signals = Signals()

    @Slot()
    def run(self):
        try:
            with self.db_manager as mgr:
                mgr.save_measurements(self.df, self.title, self.category)
                logging.info(f"数据保存成功: {self.title}")
        except Exception as e:
            logging.error(f"数据库保存失败: {str(e)}")
            self.signals.error.emit(str(e))
        self.setAutoDelete(True)


class DatabaseManager:
    def __init__(self, db_name=".\db\data.db"):
        self.db_name = str(Path(db_name).absolute())
        self.db = None
        self.connection_name = None
        print(self.db_name)

    def __enter__(self):
        self.connection_name = f"conn_{os.getpid()}_{uuid.uuid4().hex}"
        self.db = QSqlDatabase.addDatabase("QSQLITE", self.connection_name)
        self.db.setDatabaseName(self.db_name)

        retries = 3
        for attempt in range(retries):
            if self.db.open():
                logging.info(f"Database connected: {self.db_name}")
                self._create_table()
                return self
            logging.warning(
                f"Connection failed (attempt {attempt+1}): {self.db.lastError().text()}"
            )
            time.sleep(0.5)
        raise RuntimeError(f"Database connection failed: {self.db.lastError().text()}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.db.isOpen():
                if exc_type is None:
                    self.db.commit()
                else:
                    self.db.rollback()
                self.db.close()
        finally:
            if self.connection_name:
                QSqlDatabase.removeDatabase(self.connection_name)
            self.db = None

    def _create_table(self):
        query = QSqlQuery(self.db)
        query.exec(
            """
            CREATE TABLE IF NOT EXISTS measurements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT,
                type TEXT,
                part_number TEXT,
                sequence INTEGER,
                upper_tolerance REAL,
                lower_tolerance REAL,
                part1 TEXT,
                part2 TEXT,
                part3 TEXT,
                part4 TEXT,
                file_name TEXT
            )
        """
        )

    def save_measurements(self, df, title, category):
        query = QSqlQuery(self.db)
        query.prepare(
            """
            INSERT INTO measurements (
                category, type, part_number, sequence,
                upper_tolerance, lower_tolerance,
                part1, part2, part3, part4, file_name
            ) VALUES (
                :category, :type, :part_number, :sequence,
                :upper_tolerance, :lower_tolerance,
                :part1, :part2, :part3, :part4, :file_name
            )
        """
        )
        self.db.transaction()
        try:
            for _, row in df.iterrows():
                self._bind_query_values(query, row, title, category)
                if not query.exec():
                    logging.error("Insert error: %s", query.lastError().text())
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            logging.error("Transaction rollback: %s", str(e))
        finally:
            query.finish()

    def _bind_query_values(self, query, row, title, category):
        try:
            query.bindValue(":category", str(category))
            query.bindValue(":type", str(row.get("类型", "")))
            query.bindValue(":part_number", str(row.get("编号", "")))

            sequence = self._parse_sequence(row.get("序号"))
            query.bindValue(":sequence", sequence)

            for col in ["上公差", "下公差"]:
                value = row.get(col)
                query.bindValue(
                    f":{col.lower()}", float(value) if pd.notna(value) else None
                )

            for part in ["零件1", "零件2", "零件3", "零件4"]:
                value = str(row.get(part, "")).strip()
                query.bindValue(f":{part.lower()}", value if value else None)

            query.bindValue(":file_name", str(title))
        except Exception as e:
            logging.error("Value binding error: %s", str(e))
            raise

    def _parse_sequence(self, sequence):
        try:
            return (
                int(sequence)
                if pd.notna(sequence) and str(sequence).isdigit()
                else None
            )
        except ValueError:
            return None


class Widget(QWidget):
    def __init__(self):
        super().__init__()
        ui_file = Path(__file__).parent / "report2csv.ui"
        loader = QUiLoader()
        self.ui = loader.load(ui_file, self)
        self.setWindowTitle("报告转换器 0.0.1")
        self.setWindowIcon(QIcon("icon.png"))
        self.setup_slots()
        self.db_manager = DatabaseManager()
        self.files = []

    def setup_slots(self):
        self.ui.pushButton.clicked.connect(self.get_files)
        self.ui.pushButton_2.clicked.connect(self.start_jobs)

    def start_jobs(self):
        if self.files:
            self.restart()
            pool = QThreadPool.globalInstance()
            for n, file in enumerate(self.files, start=1):
                self.ui.textEdit.append(
                    f'{QDateTime.currentDateTime().toString("yyyy-MM-dd hh:mm:ss")}, {file}'
                )
                worker = Worker(n, file)
                worker.signals.completed.connect(self.complete)
                worker.signals.started.connect(self.start)
                # worker.signals.save_data.connect(self.save_to_database)
                pool.start(worker)

    def restart(self):
        self.ui.progressBar.setValue(0)
        self.completed_jobs = []
        self.ui.pushButton_2.setEnabled(False)
        self.ui.pushButton_2.setText("运行中")
        self.ui.tableWidget.setRowCount(0)  # 清空表格

    def start(self, n):
        self.ui.listWidget.addItem(f"任务 #{n} 已启动...")
        self.ui.lineEdit.setText(f"{n}/{len(self.files)}: {Path(self.files[n-1]).name}")

    def complete(self, msg):
        n, file, number, title, icmd, icmc, category = msg
        self.ui.listWidget.addItem(f"任务 #{n} 已完成")
        self.completed_jobs.append(n)

        row = self.ui.tableWidget.rowCount()
        self.ui.tableWidget.insertRow(row)
        self.ui.tableWidget.setItem(row, 0, QTableWidgetItem(str(number)))
        self.ui.tableWidget.setItem(row, 1, QTableWidgetItem(title))
        self.ui.tableWidget.setItem(row, 2, QTableWidgetItem(f"{icmd:.2%}"))
        self.ui.tableWidget.setItem(row, 3, QTableWidgetItem(f"{icmc:.2%}"))
        self.ui.tableWidget.setItem(row, 4, QTableWidgetItem(category))
        self.ui.tableWidget.resizeColumnsToContents()

        self.ui.progressBar.setValue(len(self.completed_jobs))
        if len(self.completed_jobs) == len(self.files):
            self.ui.pushButton_2.setEnabled(True)
            self.ui.pushButton_2.setText("开始")

    def save_to_database(self, df, title, category):
        worker = DatabaseWorker(df, title, category, self.db_manager)
        worker.signals.error.connect(
            lambda err: self.ui.listWidget.addItem(f"数据库错误: {err}")
        )
        QThreadPool.globalInstance().start(worker)

    def get_files(self):
        self.files, _ = QFileDialog.getOpenFileNames(
            self,
            "打开文件",
            r"E:\Project\S32\06-零件报告\MDL\外制\敏实",
            "Excel文件 (*.xls*)",
        )
        if self.files:
            self.ui.progressBar.setMaximum(len(self.files))


if __name__ == "__main__":
    app = QApplication([])
    widget = Widget()
    widget.show()
    app.exec()
